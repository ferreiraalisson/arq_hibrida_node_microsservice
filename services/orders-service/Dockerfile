FROM node:20-alpine
WORKDIR /app

RUN apk add --no-cache openssl libc6-compat bash

COPY services/orders-service/package*.json ./

RUN npm install

COPY services/orders-service/prisma ./prisma
RUN npx prisma generate

COPY services/orders-service/src ./src
COPY common ./common

ENV PORT=3002
ENV DATABASE_URL="postgresql://admin:admin@postgres:5432/orders_db?schema=public"
ENV RABBITMQ_URL="amqp://guest:guest@rabbitmq:5672"
ENV EXCHANGE="app.topic"
ENV QUEUE="orders.q"
ENV USERS_BASE_URL="http://users:3001"
ENV HTTP_TIMEOUT_MS="2000"

EXPOSE 3002

CMD ["npm","start"]
