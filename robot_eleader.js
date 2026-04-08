require('dotenv').config(); 
const puppeteer = require('puppeteer');
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
    if (!fs.existsSync(downloadPath)) fs.mkdirSync(downloadPath);

    const fechasABuscar = obtenerFechasDinamicas();
    log.success(`\n[INIT] 🚀 ROBOT ELEADER ACTIVADO.`);

    for (const fechaActual of fechasABuscar) {
        const y = fechaActual.getUTCFullYear().toString();
        const m = (fechaActual.getUTCMonth() + 1).toString().padStart(2, '0');
        const d = fechaActual.getUTCDate().toString().padStart(2, '0');
        const fechaReporteFinal = `${y}-${m}-${d}`;
        const rutaCarpetaVirtual = `FOTOS/${y}/${m}`; 
        
        log.info(`📅 PROCESANDO FECHA: ${fechaReporteFinal}`);

const browser = await puppeteer.launch({
        headless: "new",
        args: ['--no-sandbox', '--disable-setuid-sandbox', '--disable-web-security', '--disable-features=IsolateOrigins,site-per-process']
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
            let masterExcelData = [];
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
                    await page.keyboard.press('Enter');
                    await delay(15000); 

                    await page.evaluate(() => {
                        const items = Array.from(document.querySelectorAll('a, span'));
                        const menu = items.find(el => el.textContent.trim() === 'Informes');
                        if (menu) menu.click();
                    });
                    await delay(5000); 

                    await page.evaluate(() => {
                        const items = Array.from(document.querySelectorAll('a, span'));
                        const panel = items.find(el => el.textContent.trim() === 'Panel de informe');
                        if (panel) panel.click();
                    });
                    await delay(10000); 

                    const searchInputSelector = 'input[id*="srch"], input[placeholder*="ntroduce"]';
                    await page.waitForSelector(searchInputSelector, { timeout: 20000 });
                    await page.type(searchInputSelector, nombreReporte);
                    await page.keyboard.press('Enter');
                    await delay(5000);

                    await page.evaluate((target) => {
                        const links = Array.from(document.querySelectorAll('a, td'));
                        const found = links.find(el => el.textContent.toLowerCase().includes(target.toLowerCase()));
                        if (found) found.click();
                    }, nombreReporte);
                    await delay(10000);

                    for (const frame of page.frames()) {
                        await frame.evaluate((yVal, mVal, dVal) => {
                            const inY = document.querySelectorAll('input[placeholder="AAAA"], input[placeholder="YYYY"]');
                            const inM = document.querySelectorAll('input[placeholder="MM"]');
                            const inD = document.querySelectorAll('input[placeholder="DD"]');
                            for (let i = 0; i < inY.length; i++) {
                                if (inY[i]) inY[i].value = yVal;
                                if (inM[i]) inM[i].value = mVal;
                                if (inD[i]) { inD[i].value = dVal; inD[i].dispatchEvent(new Event('change', {bubbles:true})); }
                            }
                        }, y, m, d).catch(()=>{});
                    }
                    await delay(5000);

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
                        await delay(5000);
                    }

                    if (filePath) {
                        const zip = new AdmZip(filePath);
                        const zipEntries = zip.getEntries();
                        const excelEntry = zipEntries.find(e => e.entryName.toLowerCase().endsWith('.xlsx'));
                        if (excelEntry) {
                            let workbook = xlsx.read(excelEntry.getData(), { type: 'buffer' });
                            let rawData = xlsx.utils.sheet_to_json(workbook.Sheets[workbook.SheetNames[0]]);
                            for (let row of rawData) {
                                let photoLink = row['Fotos'] || row['Foto'] || "";
                                if (photoLink.includes('http')) {
                                    let imgBuffer = await descargarFoto(photoLink);
                                    if (imgBuffer) {
                                        let imgName = `FOTO_${Date.now()}_${Math.random().toString(36).substring(7)}.jpg`;
                                        let azureUrl = await subirAAzure(imgName, imgBuffer, rutaCarpetaVirtual);
                                        masterExcelData.push({ ...row, 'Link Azure': azureUrl, 'Unidad': unidadNegocioActual });
                                    }
                                }
                            }
                        }
                        fs.unlinkSync(filePath);
                        log.success(`Reporte ${nombreReporte} procesado.`);
                    }
                } catch (err) { log.error(`Error en ${nombreReporte}: ${err.message}`); }
                await page.close(); 
            } 

            if (masterExcelData.length > 0) {
                let newWb = xlsx.utils.book_new();
                let newWs = xlsx.utils.json_to_sheet(masterExcelData);
                xlsx.utils.book_append_sheet(newWb, newWs, "Consolidado");
                let buffer = xlsx.write(newWb, { type: 'buffer', bookType: 'xlsx' });
                await subirAAzure(`Master_${fechaReporteFinal}.xlsx`, buffer, 'EXCEL_DIARIO');
            }
        } finally { await browser.close(); }
    }
    log.success(`✅ EXTRACCIÓN DIARIA COMPLETADA.`);
    process.exit(0);
})();
