FROM ghcr.io/puppeteer/puppeteer:latest

USER root
WORKDIR /usr/src/app

COPY package*.json ./
RUN npm install --no-cache

COPY . .

CMD ["node", "robot_eleader.js"]
