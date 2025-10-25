// export async function createChannel(url, exchange) {
//   const conn = await amqplib.connect(url);
//   const ch = await conn.createChannel();
//   await ch.assertExchange(exchange, 'topic', { durable: true });
//   return { conn, ch };
// }

// amqp.js


import amqplib from 'amqplib';

const MAX_RETRIES = 10;
const BASE_RETRY_DELAY_MS = 5000; // 5 segundos

async function connectWithRetry(url) {
  
  for (let retries = 0; retries < MAX_RETRIES; retries++){
    try {
      const conn = await amqplib.connect(url);
      
      // conn.on('error', (err) => {
      //   console.error('[AMQP] connection error', err.message);
      //   process.exit(1); // Encerra para que o Docker possa reiniciar o contêiner
      // });
      // conn.on('close', () => {
      //   console.error('[AMQP] connection closed');
      //   process.exit(1);
      // });
      
      console.log('[AMQP] Connected successfully!');
      return conn;
    } catch (err) {
      // retries++;
      // console.error(`[AMQP] Connection failed. Retrying in ${RETRY_DELAY_MS / 1000}s... (${retries}/${MAX_RETRIES})`);
      // if (retries >= MAX_RETRIES) {
      //   console.error('[AMQP] Max retries reached. Exiting.');
      //   throw err;
      // }
      // await new Promise(resolve => setTimeout(resolve, RETRY_DELAY_MS));

      if (retries === MAX_RETRIES - 1){
        console.error('[AMQP] All retries failed. Giving up.');
        throw err;
      }

      const delay = BASE_RETRY_DELAY_MS * Math.pow(2, retries);
      // (Opcional) Adicionar "jitter" (um tempinho aleatório) para não sobrecarregar
      const jitter = Math.floor(Math.random() * 1000); // 0 a 1s
      
      console.log(`[AMQP] Retrying in ${delay + jitter}ms...`);
      
      // Espera o tempo calculado antes de ir para a próxima volta do loop
      await new Promise(resolve => setTimeout(resolve, delay + jitter));
    }
  }
}

export async function createChannel(url, exchange) {
  // A conexão agora usará a lógica de retry
  const conn = await connectWithRetry(url);
  
  // Lógica para fechar a conexão graciosamente ao encerrar o processo
  process.once('SIGINT', async () => {
    await conn.close();
  });

  const ch = await conn.createChannel();
  await ch.assertExchange(exchange, 'topic', { durable: true });
  return { conn, ch };
}