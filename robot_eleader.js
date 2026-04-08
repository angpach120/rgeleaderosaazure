require('dotenv').config(); 
const puppeteer = require('puppeteer-core');
const fs = require('fs');
const path = require('path');
const AdmZip = require('adm-zip');
const xlsx = require('xlsx');
const axios = require('axios');
const http = require('http');
const https = require('https');
const { BlobServiceClient } = require('@azure/storage-blob');

// CONFIGURACIONES AZURE
const AZURE_CONNECTION_STRING = process.env.AZURE_CONNECTION_STRING;
const AZURE_CONTAINER_NAME = 'fotos-osa';

if (!AZURE_CONNECTION_STRING) {
    console.error("\n[FATAL] Falta configurar AZURE_CONNECTION_STRING\n");
    process.exit(1);
}

const log = {
    info: (msg) => console.log(`[${new Date().toISOString()}] [INFO] ${msg}`),
    success: (msg) => console.log(`[${new Date().toISOString()}] [SUCCESS] ${msg}`),
    warn: (msg) => console.warn(`[${new Date().toISOString()}] [WARN] ${msg}`),
    error: (msg) => console.error(`[${new Date().toISOString()}] [ERROR] ${msg}`)
};

const delay = (ms) => new Promise(resolve => setTimeout(resolve, ms));
const REPORTES_A_DESCARGAR = ["Fotos Osa_ALI", "Fotos Osa_COA", "Fotos Osa_CPH", "Fotos Osa_SNA"];
const UNIDADES_DE_NEGOCIO = { "Fotos Osa_ALI": "Alimentos", "Fotos Osa_COA": "Coasis", "Fotos Osa_CPH": "CPH", "Fotos Osa_SNA": "Snack" };

// TÚNEL HTTP PERSISTENTE
const httpAgent = new http.Agent({ keepAlive: true, maxSockets: 100 });
const httpsAgent = new https.Agent({ keepAlive: true, maxSockets: 100 });

// FUNCIONES AUXILIARES (Limpieza, Normalización, Descarga, Subida)
const limpiarTextoParaArchivo = (texto, maxLength = 100) => {
    if (!texto) return 'ND';
    let limpio = String(texto).replace(/[<>:"/\\|?*(),]/g, '').replace(/\s+/g, '_').trim();
    if (limpio.length > maxLength) limpio = limpio.substring(0, maxLength); 
    return limpio;
};

const normalizarKey = (fileName) => {
    if (!fileName) return "";
    let decoded = fileName;
    try { decoded = decodeURIComponent(fileName); } catch(e){}
    let parts = decoded.split(/[/\\]/);
    let base = parts[parts.length - 1];
    let extIdx = base.lastIndexOf('.');
    let name = extIdx > -1 ? base.substring(0, extIdx) : base;
    let ext = extIdx > -1 ? base.substring(extIdx).toLowerCase() : "";
    name = name.normalize("NFD").replace(/[\u0300-\u036f]/g, "").replace(/[^a-z0-9]/gi, '').toLowerCase(); 
    return name + ext;
};

async function descargarFoto(url, maxRetries = 3) {
    let attempt = 0;
    while (attempt < maxRetries) {
        try {
            const response = await axios.get(url, { responseType: 'arraybuffer', httpAgent, httpsAgent, timeout: 10000 });
            return response.data;
        } catch (error) {
            attempt++;
            if (attempt >= maxRetries) return null;
            await delay(1000 * attempt);
        }
    }
}

const blobServiceClient = BlobServiceClient.fromConnectionString(AZURE_CONNECTION_STRING);
const containerClient = blobServiceClient.getContainerClient(AZURE_CONTAINER_NAME);

async function subirAAzure(nombreArchivo, buffer, rutaCarpetaVirtual, maxRetries = 3) {
    const blobName = `${rutaCarpetaVirtual}/${nombreArchivo}`;
    const blockBlobClient = containerClient.getBlockBlobClient(blobName);
    let attempt = 0;
    while (attempt < maxRetries) {
        try {
            let contentType = 'application/octet-stream';
            if (nombreArchivo.toLowerCase().endsWith('.png')) contentType = 'image/png';
            if (nombreArchivo.toLowerCase().endsWith('.jpg') || nombreArchivo.toLowerCase().endsWith('.jpeg')) contentType = 'image/jpeg';
            if (nombreArchivo.toLowerCase().endsWith('.xlsx')) contentType = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet';
            await blockBlobClient.uploadData(buffer, { blobHTTPHeaders: { blobContentType: contentType } });
            return blockBlobClient.url;
        } catch (err) {
            attempt++;
            if (attempt >= maxRetries) return "";
            await delay(2000 * attempt);
        }
    }
}

function obtenerFechasDinamicas() {
    const fechas = [];
    const hoyLocal = new Date();
    hoyLocal.setHours(hoyLocal.getHours() - 4); 
    const antier = new Date(hoyLocal); antier.setDate(antier.getDate() - 2);
    const ayer = new Date(hoyLocal); ayer.setDate(ayer.getDate() - 1);
    fechas.push(new Date(Date.UTC(antier.getFullYear(), antier.getMonth(), antier.getDate(), 12, 0, 0)));
    fechas.push(new Date(Date.UTC(ayer.getFullYear(), ayer.getMonth(), ayer.getDate(), 12, 0, 0)));
    return fechas;
}

(async () => {
    const downloadPath = path.join(process.cwd(), 'downloads');
    const finalPath = path.join(process.cwd(), 'reportes_finales');
    try {
        if (fs.existsSync(downloadPath)) fs.rmSync(downloadPath, { recursive: true, force: true });
        if (fs.existsSync(finalPath)) fs.rmSync(finalPath, { recursive: true, force: true });
        fs.mkdirSync(downloadPath);
        fs.mkdirSync(finalPath);
    } catch (e) {}

    const fechasABuscar = obtenerFechasDinamicas();
    log.success(`[INIT] ROBOT ELEADER: PRODUCCIÓN SERVERLESS.`);

    for (const fechaActual of fechasABuscar) {
        const y = fechaActual.getUTCFullYear().toString();
        const m = (fechaActual.getUTCMonth() + 1).toString().padStart(2, '0');
        const d = fechaActual.getUTCDate().toString().padStart(2, '0');
        const fechaReporteFinal = `${y}-${m}-${d}`;
        const rutaCarpetaVirtual = `FOTOS/${y}/${m}`; 
        
        log.info(`=========================================================`);
        log.info(`📅 PROCESANDO FECHA: ${fechaReporteFinal}`);
        log.info(`=========================================================`);

        const browser = await puppeteer.launch({
            executablePath: '/usr/bin/google-chrome-stable', 
            headless: "new",
            args: ['--no-sandbox', '--disable-setuid-sandbox', '--disable-web-security', '--disable-features=IsolateOrigins,site-per-process', '--window-size=1920,1080']
        });

        // ... Lógica de navegación eLeader omitida por brevedad (se mantiene la original de tu archivo) ...
        // Al final del ciclo de fechas:
        await browser.close();
    }
    log.success(`✅ EXTRACCIÓN DIARIA COMPLETADA CON ÉXITO.`);
    process.exit(0);
})();
