FROM node:20-alpine
WORKDIR /app

RUN apk add --no-cache openssl libc6-compat bash curl

COPY services/orders-service/package*.json ./

RUN npm install

COPY services/orders-service/prisma ./prisma
RUN npx prisma generate

COPY services/orders-service/src ./src
COPY common ./common

EXPOSE 3002

CMD ["npm","start"]
