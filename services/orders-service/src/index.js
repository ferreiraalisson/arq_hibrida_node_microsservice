import express from 'express';
import morgan from 'morgan';
import fetch from 'node-fetch';
import { nanoid } from 'nanoid';
import { createChannel } from './amqp.js';
import { ROUTING_KEYS } from '../common/events.js';
import { PrismaClient } from '@prisma/client';
import opossum from 'opossum';

const app = express();
const prisma = new PrismaClient();

app.use(express.json());
app.use(morgan('dev'));

const PORT = process.env.PORT || 3002;
const USERS_BASE_URL = process.env.USERS_BASE_URL || 'http://ms_users:3001';
const HTTP_TIMEOUT_MS = Number(process.env.HTTP_TIMEOUT_MS || 2000);
const RABBITMQ_URL = process.env.RABBITMQ_URL || 'amqp://guest:guest@ms_rabbitmq:5672';
const EXCHANGE = process.env.EXCHANGE || 'app.topic';
const QUEUE = process.env.QUEUE || 'orders.q';
const ROUTING_KEY_USER_CREATED = process.env.ROUTING_KEY_USER_CREATED || ROUTING_KEYS.USER_CREATED;
const BASE_HTTP_RETRY_DELAY = 500;
const MAX_HTTP_RETRIES = 3;
// In-memory "DB"
// const orders = new Map();
// In-memory cache de usuários (preenchido por eventos)
const userCache = new Map();

let amqp = null;
(async () => {
  try {
    amqp = await createChannel(RABBITMQ_URL, EXCHANGE);
    console.log('[orders] AMQP connected');

    // Bind de fila para consumir eventos user.created
    await amqp.ch.assertQueue(QUEUE, { durable: true });
    await amqp.ch.bindQueue(QUEUE, EXCHANGE, ROUTING_KEY_USER_CREATED);

    amqp.ch.consume(QUEUE, msg => {
      if (!msg) return;
      try {
        const user = JSON.parse(msg.content.toString());
        // idempotência simples: atualiza/define
        userCache.set(user.id, user);
        console.log('[orders] consumed event user.created -> cached', user.id);
        amqp.ch.ack(msg);
      } catch (err) {
        console.error('[orders] consume error:', err.message);
        amqp.ch.nack(msg, false, false); // descarta em caso de erro de parsing (aula: discutir DLQ)
      }
    });
  } catch (err) {
    console.error('[orders] AMQP connection failed:', err.message);
  }
})();

app.get('/health', (req, res) => res.json({ ok: true, service: 'orders' }));

app.get('/', async (req, res) => {
  // res.json(Array.from(orders.values()));
  const list = await prisma.order.findMany();
  res.json(list);
});

async function fetchWithBackoff(url, ms) {
  for ( let retries = 0; retries < MAX_HTTP_RETRIES; retries++){
    try {

      const controller = new AbortController();
      const timeoutId = setTimeout(() => controller.abort(), ms);

      const res = await fetch(url, { signal: controller.signal });
      clearTimeout(timeoutId);

      if(res.status >= 500){
        throw new Error(`Users service (servidor) falhou com status ${res.status}`);
      }

      return res;

    } catch (error) {
      console.warn(`[HTTP Fetch] Falha ao chamar ${url}. Tentativa ${retries + 1}.`)
    
      if (retries === MAX_HTTP_RETRIES - 1) {
        console.error('[HTTP Fetch] Todas as tentativas falharam.');
        throw err; // Lança o erro na última tentativa
      }

      const delay = BASE_HTTP_RETRY_DELAY * Math.pow(2, retries);
      const jitter = Math.floor(Math.random() * 200); // 0 a 0.2s
      
      console.warn(`[HTTP Fetch] Tentando de novo em ${delay + jitter}ms...`);
      await new Promise(resolve => setTimeout(resolve, delay + jitter));
    
    }
  }
  // const controller = new AbortController();
  // const id = setTimeout(() => controller.abort(), ms);
  // try {
  //   const res = await fetch(url, { signal: controller.signal });
  //   return res;
  // } finally {
  //   clearTimeout(id);
  // }
}

// Esta função agora usa nosso fetch com retry
async function callUserService(userId) {
  const url = `${USERS_BASE_URL}/${userId}`;
  
  // 1. Chama a função com retry/backoff
  const resp = await fetchWithBackoff(url, HTTP_TIMEOUT_MS);

  // 2. Trata a resposta (o 'fetchWithBackoff' já filtrou os 5xx)
  if (!resp.ok) { // 404 (não achou), 400 (inválido)
    return { error: 'usuário inválido', status: resp.status };
  }
  
  return await resp.json(); // Sucesso!
}

// CONFIGURAÇÃO DO CIRCUIT BREAKER (OPOSSUM)
const breakerOptions = {
  timeout: HTTP_TIMEOUT_MS * (MAX_HTTP_RETRIES + 1), // O timeout do disjuntor deve ser MAIOR que todas as retentativas
  errorThresholdPercentage: 50, // Se 50% das chamadas falharem, o disjuntor "abre"
  resetTimeout: 10000 // Depois de 10s "aberto", ele tenta fechar
};

// Embrulha a função 'callUserService' com o disjuntor
const breaker = new opossum(callUserService, breakerOptions);

// Define o FALLBACK (Plano B)
breaker.fallback(async (userId) => {
  console.warn('[orders] CIRCUIT BREAKER FALLBACK! Tentando cache...', userId);
  if (userCache.has(userId)) {
    return userCache.get(userId); // Retorna do cache se o disjuntor estiver aberto
  }
  const err = new Error('Serviço indisponível e usuário não encontrado no cache');
  err.code = 'SERVICE_UNAVAILABLE';
  throw err;
});

// Logs para sabermos o que o disjuntor está fazendo
breaker.on('open', () => console.error('[orders] Disjuntor ABERTO para Users-Service.'));
breaker.on('close', () => console.log('[orders] Disjuntor FECHADO para Users-Service.'));
breaker.on('fallback', (result) => console.warn('[orders] Fallback acionado.', result));

app.post('/', async (req, res) => {
  const { userId, items, total } = req.body || {};
  if (!userId || !Array.isArray(items) || typeof total !== 'number') {
    return res.status(400).json({ error: 'userId, items[], total<number> são obrigatórios' });
  }

  const user = await breaker.fire(userId);

  if (user.error) {
    return res.status(user.status || 400).json({ error: user.error });
  }
  const id = `o_${nanoid(6)}`;

  // const order = { id, userId, items, total, status: 'created', createdAt: new Date().toISOString() };
  // orders.set(id, order);
  
  try {

    const order = await prisma.order.create({
      data: {
        id: id,
        userId: userId,
        items: items,
        total: total,
      }
    });
    // (Opcional) publicar evento order.created
    try {
      if (amqp?.ch) {
        amqp.ch.publish(EXCHANGE, ROUTING_KEYS.ORDER_CREATED, Buffer.from(JSON.stringify(order)), { persistent: true });
        console.log('[orders] published event:', ROUTING_KEYS.ORDER_CREATED, order.id);
      }
    } catch (err) {
      console.error('[orders] publish error:', err.message);
    }
    res.status(201).json(order);
  } catch (error) {
    if (err.code === 'SERVICE_UNAVAILABLE') {
      return res.status(503).json({ error: err.message });
    }
    console.error('[prisma] create error:', error);
    res.status(500).json({ error: 'Could not create order' });
  }
});

app.delete('/:id', async (req, res) => {
  const id = req.params.id;

  // const order = orders.get(id);
  // if (!order) {
  //   return res.status(404).json({ error: 'not found' });
  // }

  // orders.delete(id);

  try {
      const order = await prisma.order.update({
      where: { id },
      data: { status: 'cancelled' }
    });

    try {
      if (amqp?.ch) {
        amqp.ch.publish(EXCHANGE, ROUTING_KEYS.ORDER_CANCELLED, Buffer.from(JSON.stringify(order)), { persistent: true });
        console.log('[orders] published event:', ROUTING_KEYS.ORDER_CANCELLED, order.id);
      }
    } catch (err) {
      console.error('[orders] publish error:', err.message);
    }

    res.status(200).json({ message: 'order cancelled', id });
  } catch (error) {
      if (err.code === 'P2025') return res.status(404).json({ error: 'not found' });
      console.error('[orders] cancel error:', err);
      res.status(500).json({ error: 'Could not delete order' });
  }  
});

app.listen(PORT, '0.0.0.0', () => {
  console.log(`[orders] listening on http://0.0.0.0:${PORT}`);
  console.log(`[orders] users base url: ${USERS_BASE_URL}`);
});
