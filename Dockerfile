# Usamos la imagen oficial de Google Chrome y Node.js (Especial para Puppeteer)
FROM ghcr.io/puppeteer/puppeteer:latest

WORKDIR /usr/src/app

COPY package*.json ./
RUN npm install

COPY robot_eleader.js .

CMD ["node", "robot_eleader.js"]
