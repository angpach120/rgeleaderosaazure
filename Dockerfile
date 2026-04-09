FROM node:20-bullseye-slim

# 1. Instalamos el navegador Chromium oficial de Linux
RUN apt-get update && apt-get install -y \
    chromium \
    fonts-ipafont-gothic fonts-wqy-zenhei fonts-thai-tlwg fonts-kacst fonts-freefont-ttf \
    --no-install-recommends \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /usr/src/app

COPY package*.json ./

# 2. 🔥 EL TRUCO MÁGICO: Bloqueamos la descarga infinita de Puppeteer 🔥
ENV PUPPETEER_SKIP_DOWNLOAD=true
ENV PUPPETEER_SKIP_CHROMIUM_DOWNLOAD=true

# 3. Instalamos librerías (ahora pasará al instante sin colgarse)
RUN npm install --no-fund --no-audit

COPY . .

CMD ["node", "robot_eleader.js"]
