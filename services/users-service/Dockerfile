FROM node:20-alpine
WORKDIR /app

RUN apk add --no-cache curl

RUN apk add --no-cache openssl libc6-compat

COPY services/users-service/package.json ./package.json
COPY services/users-service/prisma ./prisma

RUN npm install 


RUN npx prisma generate

COPY services/users-service/src ./src
COPY common ./common

ENV PORT=3001
EXPOSE 3001
CMD ["npm","start"]
