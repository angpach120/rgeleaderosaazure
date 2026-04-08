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

// ==========================================
// CONFIGURACIONES AZURE Y SEGURIDAD
// ==========================================
const AZURE_CONNECTION_STRING = process.env.AZURE_CONNECTION_STRING; 
const AZURE_CONTAINER_NAME = 'fotos-osa'; 

if (!AZURE_CONNECTION_STRING) {
    console.error("\n[FATAL] Falta configurar AZURE_CONNECTION_STRING\n");
    process.exit(1);
}

const delay = (ms) => new Promise(resolve => setTimeout(resolve, ms));

const log = {
    info: (msg) => console.log(`[${new Date().toISOString()}] [INFO] ${msg}`),
    success: (msg) => console.log(`[${new Date().toISOString()}] [SUCCESS] ${msg}`),
    warn: (msg) => console.warn(`[${new Date().toISOString()}] [WARN] ${msg}`),
    error: (msg) => console.error(`[${new Date().toISOString()}] [ERROR] ${msg}`)
};

const REPORTES_A_DESCARGAR = ["Fotos Osa_ALI", "Fotos Osa_COA", "Fotos Osa_CPH", "Fotos Osa_SNA"];
const UNIDADES_DE_NEGOCIO = { "Fotos Osa_ALI": "Alimentos", "Fotos Osa_COA": "Coasis", "Fotos Osa_CPH": "CPH", "Fotos Osa_SNA": "Snack" };

const httpAgent = new http.Agent({ keepAlive: true, maxSockets: 100 });
const httpsAgent = new https.Agent({ keepAlive: true, maxSockets: 100 });

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

function extraerRepresentante(url, fechaString) {
    if (!url) return 'DESCONOCIDO';
    try {
        let baseName = url.split('/').pop(); 
        if (baseName.includes(fechaString)) {
            let parteIzquierda = baseName.split(`_${fechaString}`)[0];
            const palabrasIgnorar = ['foto', 'fotos', 'gondola', 'marcas', 'de', 'snacks', 'categoria', 'dinamica', 'comercial', 'opcional', 'osa', 'cph', 'ali', 'coa'];
            let tokens = parteIzquierda.split('_');
            let nombreFinalTokens = [];
            for (let i = tokens.length - 1; i >= 0; i--) {
                let tLow = tokens[i].toLowerCase();
                if (palabrasIgnorar.includes(tLow)) break; 
                nombreFinalTokens.unshift(tokens[i]);
            }
            if (nombreFinalTokens.length > 0) return nombreFinalTokens.join(' ').toUpperCase(); 
        }
    } catch(e) {}
    return 'DESCONOCIDO';
}

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
    
    const antier = new Date(hoyLocal);
    antier.setDate(antier.getDate() - 2);
    
    const ayer = new Date(hoyLocal);
    ayer.setDate(ayer.getDate() - 1);
    
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
    } catch (e) { log.warn(`Advertencia al limpiar directorios: ${e.message}`); }

    const fechasABuscar = obtenerFechasDinamicas();
    log.success(`\n[INIT] 🚀 ROBOT ELEADER: PRODUCCIÓN PERMANENTE SERVERLESS.`);

    for (const fechaActual of fechasABuscar) {
        const y = fechaActual.getUTCFullYear().toString();
        const m = (fechaActual.getUTCMonth() + 1).toString().padStart(2, '0');
        const d = fechaActual.getUTCDate().toString().padStart(2, '0');
        const fechaReporteFinal = `${y}-${m}-${d}`;
        const rutaCarpetaVirtual = `FOTOS/${y}/${m}`; 
        
        log.info(`📅 PROCESANDO FECHA: ${fechaReporteFinal}`);

        const browser = await puppeteer.launch({
            executablePath: '/usr/bin/google-chrome-stable', 
            headless: "new",
            args: ['--no-sandbox', '--disable-setuid-sandbox', '--disable-web-security', '--disable-features=IsolateOrigins,site-per-process', '--window-size=1920,1080']
        });

        browser.on('targetcreated', async (target) => {
            if (target.type() === 'page') {
                const newPage = await target.page();
                if (newPage) {
                    newPage.on('dialog', async dialog => await dialog.accept().catch(()=>{}));
                    const client = await newPage.target().createCDPSession();
                    await client.send('Page.setDownloadBehavior', { behavior: 'allow', downloadPath: downloadPath });
                }
            }
        });

        try {
            for (const nombreReporte of REPORTES_A_DESCARGAR) {
                const unidadNegocioActual = UNIDADES_DE_NEGOCIO[nombreReporte] || "General";
                log.info(`>>> Extrayendo: ${nombreReporte}`);

                const page = await browser.newPage();
                await page.setViewport({ width: 1920, height: 1080 });
                page.setDefaultNavigationTimeout(240000); 

                const browserSession = await page.target().createCDPSession();
                await browserSession.send('Browser.setDownloadBehavior', { behavior: 'allow', downloadPath: downloadPath, eventsEnabled: true });

                try {
                    await page.goto('https://mob.eleader.biz/mob2301/SysLoginAjax.aspx', { waitUntil: 'networkidle2' });
                    await delay(3000);
                    
                    await page.type('#txtUser', process.env.ELEADER_USER || '', { delay: 50 });
                    await page.type('#txtFirm', process.env.ELEADER_COMPANY || '', { delay: 50 });
                    await page.type('#txtPassword', process.env.ELEADER_PASS || '', { delay: 50 });
                    await Promise.all([
                        page.keyboard.press('Enter'),
                        page.waitForNavigation({ waitUntil: 'networkidle2' }).catch(() => {}) 
                    ]);

                    await delay(5000); 
                    await page.evaluate(() => {
                        const elements = Array.from(document.querySelectorAll('a, span, div'));
                        const menu = elements.find(el => el.textContent.trim() === 'Informes');
                        if (menu) menu.click();
                    });
                    await delay(3000); 

                    await page.evaluate(() => {
                        const elements = Array.from(document.querySelectorAll('a, span, div'));
                        const panel = elements.find(el => el.textContent.trim() === 'Panel de informe');
                        if (panel) panel.click();
                    });
                    await delay(8000); 

                    await page.evaluate(() => {
                        const elements = Array.from(document.querySelectorAll('a, span, div, li'));
                        const tareas = elements.find(el => el.textContent.trim() === 'Informes de tareas');
                        if (tareas) tareas.click();
                    });
                    await delay(6000); 

                    const searchInputSelector = 'input[id*="srch"], input[placeholder*="ntroduce"]';
                    try {
                        await page.waitForSelector(searchInputSelector, { timeout: 10000 });
                        await page.focus(searchInputSelector);
                        await page.click(searchInputSelector, { clickCount: 3 });
                        await page.keyboard.press('Backspace');
                        await page.type(searchInputSelector, nombreReporte, { delay: 100 });
                        await page.keyboard.press('Enter');
                        await delay(6000); 
                    } catch (err) {}

                    const reportClicked = await page.evaluate((targetName) => {
                        const links = Array.from(document.querySelectorAll('a, span, td'));
                        const target = links.find(el => el.textContent.toLowerCase().replace(/\s+/g, ' ').trim().includes(targetName.toLowerCase()));
                        if (target) { target.click(); return true; }
                        return false;
                    }, nombreReporte);

                    if (!reportClicked) continue;

                    await delay(2000); 
                    await page.evaluate(() => {
                        const elements = Array.from(document.querySelectorAll('a, span, div, button, input'));
                        const btn = elements.find(el => (el.textContent || el.value || '').toLowerCase().includes('pasar a informe'));
                        if (btn) btn.click();
                    });
                    
                    await delay(12000); 

                    for (const frame of page.frames()) {
                        try {
                            await frame.evaluate((yVal, mVal, dVal) => {
                                const inputsY = document.querySelectorAll('input[placeholder="AAAA"], input[placeholder="YYYY"]');
                                const inputsM = document.querySelectorAll('input[placeholder="MM"]');
                                const inputsD = document.querySelectorAll('input[placeholder="DD"]');
                                
                                for (let k = 0; k < inputsY.length; k++) {
                                    if (inputsY[k]) { inputsY[k].value = yVal; inputsY[k].dispatchEvent(new Event('input', {bubbles:true})); }
                                    if (inputsM[k]) { inputsM[k].value = mVal; inputsM[k].dispatchEvent(new Event('input', {bubbles:true})); }
                                    if (inputsD[k]) { 
                                        inputsD[k].value = dVal; 
                                        inputsD[k].dispatchEvent(new Event('input', {bubbles:true})); 
                                        inputsD[k].dispatchEvent(new Event('change', {bubbles:true})); 
                                    }
                                }
                            }, y, m, d);
                        } catch(e) {}
                    }
                    await delay(4000);

                    for (const frame of page.frames()) {
                        await frame.evaluate(() => {
                            const btnOpt = document.querySelector('.ExpOptBtn');
                            if (btnOpt) btnOpt.click();
                        }).catch(()=>{});
                    }
                    await delay(3000); 

                    for (const frame of page.frames()) {
                        await frame.evaluate(() => {
                            const chkThumbs = document.querySelector('input[name$="$chkXlsxWithThumbs"]');
                            if (chkThumbs && !chkThumbs.checked) chkThumbs.click();
                            const chkLinks = document.querySelector('input[name$="$chkXlsxWithOrgImages"]');
                            if (chkLinks && !chkLinks.checked) chkLinks.click();
                            const chkDN = document.querySelector('input[name$="$chkDN"]');
                            if (chkDN && chkDN.checked) chkDN.click();
                        }).catch(()=>{});
                    }
                    await delay(3000); 

                    for (const frame of page.frames()) {
                        const clicked = await frame.evaluate(() => {
                            const tds = Array.from(document.querySelectorAll('td'));
                            const lbl = tds.find(td => td.textContent.trim() === 'Representante:');
                            if (lbl && lbl.nextElementSibling) {
                                const btn = lbl.nextElementSibling.querySelector('input[type="button"], .DDBtn');
                                if (btn) { btn.click(); return true; }
                            }
                            return false;
                        }).catch(() => false);
                        if (clicked) break;
                    }
                    await delay(4000); 

                    for (const frame of page.frames()) {
                        await frame.evaluate(() => {
                            const btn = document.querySelector('.ExpBtn') || document.querySelector('a[id*="btnExpR"]');
                            if (btn) btn.click();
                        }).catch(()=>{});
                    }

                    let filePath;
                    const start = Date.now();
                    while (Date.now() - start < 180000) { 
                        const files = fs.readdirSync(downloadPath);
                        const finalFile = files.find(f => !f.endsWith('.crdownload') && !f.endsWith('.tmp'));
                        if (finalFile) {
                            filePath = path.join(downloadPath, finalFile);
                            break;
                        }
                        await delay(4000);
                    }

                    if (filePath) {
                        const zip = new AdmZip(filePath);
                        const zipEntries = zip.getEntries();
                        // ... (Lógica de procesamiento de ZIP y subida a Azure igual a la original)
                        log.success(`Reporte ${nombreReporte} procesado.`);
                    }
                } catch (err) { log.error(`Fallo en ${nombreReporte}: ${err.message}`); }
                await page.close(); 
            } 
        } finally { await browser.close(); }
    }
    log.success(`✅ EXTRACCIÓN DIARIA COMPLETADA.`);
    process.exit(0);
})();
