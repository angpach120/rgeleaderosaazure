require('dotenv').config(); 
// Usamos puppeteer-core para nuestra imagen ligera de Docker
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
// CONFIGURACIONES AZURE Y SEGURIDAD DUALES
// ==========================================
const AZURE_CONNECTION_STRING = process.env.AZURE_CONNECTION_STRING; 
const AZURE_CONTAINER_OSA = 'fotos-osa'; 
const AZURE_CONTAINER_AC = 'fotos-ac';
const AZURE_CONTAINER_PROMO = 'fotos-promo';

if (!AZURE_CONNECTION_STRING) {
    console.error("\n[FATAL] Falta configurar AZURE_CONNECTION_STRING en los Secrets de GitHub.\n");
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

// 🚀 TURBINA 3: TÚNEL HTTP PERSISTENTE (Evita el agotamiento de puertos TCP)
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
    name = name.normalize("NFD").replace(/[\u0300-\u036f]/g, ""); 
    name = name.replace(/[^a-z0-9]/gi, '').toLowerCase(); 
    return name + ext;
};

function extraerRepresentante(url, fechaString) {
    if (!url) return 'DESCONOCIDO';
    try {
        let baseName = url.split('/').pop(); 
        if (baseName.includes(fechaString)) {
            let parteIzquierda = baseName.split(`_${fechaString}`)[0];
const palabrasIgnorar = ['foto', 'fotos', 'gondola', 'marcas', 'de', 'snacks', 'categoria', 'dinamica', 'comercial', 'opcional', 'osa', 'cph', 'ali', 'coa', 'promo', 'promociones'];
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

// 🛠️ CIMIENTOS CORREGIDOS: Clientes para los tres contenedores
const blobServiceClient = BlobServiceClient.fromConnectionString(AZURE_CONNECTION_STRING);
const containerClientOsa = blobServiceClient.getContainerClient(AZURE_CONTAINER_OSA);
const containerClientAc = blobServiceClient.getContainerClient(AZURE_CONTAINER_AC);
const containerClientPromo = blobServiceClient.getContainerClient(AZURE_CONTAINER_PROMO); // <-- AGREGAR ESTA LÍNEA

// 🛠️ CIMIENTOS CORREGIDOS: La función ahora acepta targetContainerClient
async function subirAAzure(nombreArchivo, buffer, rutaCarpetaVirtual, targetContainerClient, maxRetries = 3) {
    const blobName = `${rutaCarpetaVirtual}/${nombreArchivo}`;
    const blockBlobClient = targetContainerClient.getBlockBlobClient(blobName);
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

// 🧠 MOTOR DE FECHAS DINÁMICAS (Antier, Ayer y Hoy)
function obtenerFechasDinamicas() {
    const fechas = [];
    const hoyLocal = new Date();
    hoyLocal.setHours(hoyLocal.getHours() - 4); 
    const antier = new Date(hoyLocal); antier.setDate(antier.getDate() - 2);
    const ayer = new Date(hoyLocal); ayer.setDate(ayer.getDate() - 1);
    const hoy = new Date(hoyLocal);
    
    fechas.push(new Date(Date.UTC(antier.getFullYear(), antier.getMonth(), antier.getDate(), 12, 0, 0)));
    fechas.push(new Date(Date.UTC(ayer.getFullYear(), ayer.getMonth(), ayer.getDate(), 12, 0, 0)));
    fechas.push(new Date(Date.UTC(hoy.getFullYear(), hoy.getMonth(), hoy.getDate(), 12, 0, 0)));
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
    log.success(`\n[INIT] 🚀 ROBOT ELEADER (EXTRACCIÓN DIARIA) ACTIVADO.`);
    log.info(`Procesando bloque de ${fechasABuscar.length} días...\n`);

    for (const fechaActual of fechasABuscar) {
        const y = fechaActual.getUTCFullYear().toString();
        const m = (fechaActual.getUTCMonth() + 1).toString().padStart(2, '0');
        const d = fechaActual.getUTCDate().toString().padStart(2, '0');
        const fechaReporteFinal = `${y}-${m}-${d}`;
        const rutaCarpetaVirtual = `FOTOS/${y}/${m}`; 
        
        log.info(`=========================================================`);
        log.info(`📅 FECHA EN PROCESO: ${fechaReporteFinal}`);
        log.info(`=========================================================`);

        let masterExcelData = [];
        const browser = await puppeteer.launch({
            executablePath: '/usr/bin/chromium', 
            headless: "new",
            args: [
                '--no-sandbox', 
                '--disable-setuid-sandbox', 
                '--disable-web-security', 
                '--disable-features=IsolateOrigins,site-per-process', 
                '--window-size=1920,1080',
                '--lang=es-ES,es' // 🔥 OBLIGAMOS A AZURE A HABLAR EN ESPAÑOL
            ]
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
                await page.setExtraHTTPHeaders({ 'Accept-Language': 'es-ES,es;q=0.9' }); 
                await page.evaluateOnNewDocument(() => { window.name = '_eld_'; });
                page.setDefaultNavigationTimeout(240000); 

                const browserSession = await page.target().createCDPSession();
                await browserSession.send('Browser.setDownloadBehavior', { behavior: 'allow', downloadPath: downloadPath, eventsEnabled: true });

                try {
                    await page.goto('https://mob.eleader.biz/mob2301/SysLoginAjax.aspx', { waitUntil: 'networkidle2' });
                    await delay(3000);
                    
                    const txtUserExists = await page.$('#txtUser');
                    if (txtUserExists) {
                        await page.type('#txtUser', process.env.ELEADER_USER || '', { delay: 50 });
                        await page.type('#txtFirm', process.env.ELEADER_COMPANY || '', { delay: 50 });
                        await page.type('#txtPassword', process.env.ELEADER_PASS || '', { delay: 50 });
                        // 🔥 Promise.all Original de tu Turbo Pro
                        await Promise.all([
                            page.keyboard.press('Enter'),
                            page.waitForNavigation({ waitUntil: 'networkidle2' }).catch(() => {}) 
                        ]);
                    }

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

                    // 🔥 EL PASO CLAVE REVELADO POR TU CÓDIGO
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
                        await delay(1000);
                        await page.keyboard.press('Enter');
                        await delay(6000); 
                    } catch (err) {}

                    const reportClicked = await page.evaluate((targetName) => {
                        const links = Array.from(document.querySelectorAll('a, span, td'));
                        const target = links.find(el => el.textContent.toLowerCase().replace(/\s+/g, ' ').trim().includes(targetName.toLowerCase()));
                        if (target) { target.click(); return true; }
                        return false;
                    }, nombreReporte);

                    if (!reportClicked) { continue; }

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
                                const inputsY = document.querySelectorAll('input[placeholder="AAAA"], input[placeholder="YYYY"], input[placeholder="yyyy"]');
                                const inputsM = document.querySelectorAll('input[placeholder="MM"], input[placeholder="mm"]');
                                const inputsD = document.querySelectorAll('input[placeholder="DD"], input[placeholder="dd"]');
                                
                                for (let k = 0; k < inputsY.length; k++) {
                                    if (inputsY[k]) { inputsY[k].value = yVal; inputsY[k].dispatchEvent(new Event('input', {bubbles:true})); }
                                    if (inputsM[k]) { inputsM[k].value = mVal; inputsM[k].dispatchEvent(new Event('input', {bubbles:true})); }
                                    if (inputsD[k]) { 
                                        inputsD[k].value = dVal; 
                                        inputsD[k].dispatchEvent(new Event('input', {bubbles:true})); 
                                        inputsD[k].dispatchEvent(new Event('change', {bubbles:true})); 
                                        inputsD[k].dispatchEvent(new Event('blur', {bubbles:true})); 
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
                        try {
                            const col1InputSelector = 'input[name$="$acCol1"]';
                            const inputExists = await frame.$(col1InputSelector);
                            if (inputExists) {
                                await frame.evaluate((selector) => { document.querySelector(selector).value = ''; }, col1InputSelector);
                                await inputExists.type('Nombre del objeto', { delay: 50 });
                                await delay(1000);
                                await inputExists.press('Enter');
                            }
                        } catch (err) {}
                    }
                    await delay(3000);

                    let activeFrame = null;
                    for (const frame of page.frames()) {
                        const clicked = await frame.evaluate(() => {
                            const tds = Array.from(document.querySelectorAll('td'));
                            const lbl = tds.find(td => td.textContent.trim() === 'Representante:');
                            if (lbl && lbl.nextElementSibling) {
                                const btn = lbl.nextElementSibling.querySelector('input[type="button"], .DDBtn, img');
                                if (btn) { btn.click(); return true; }
                            }
                            return false;
                        }).catch(() => false);

                        if (clicked) { activeFrame = frame; break; }
                    }
                    await delay(4000); 

                    const mapData = await activeFrame.evaluate(() => {
                        const chks = Array.from(document.querySelectorAll('input[type="checkbox"][id*="innerRealExecutor"]'));
                        if (chks.length === 0) return null;
                        const firstId = chks[0].id;
                        const baseId = firstId.substring(0, firstId.lastIndexOf('_')); 
                        return { baseId: baseId, ids: chks.map(c => c.id) };
                    });

                    if(!mapData) {
                        log.warn(`[VACÍO] Reporte en blanco. Saltando...`);
                        await page.close();
                        continue;
                    }

                    let baseIdGlobal = mapData.baseId;
                    let chkIdsGlobal = mapData.ids;
                    
                    const numParts = chkIdsGlobal.length > 50 ? 4 : 2; 
                    const chunkSize = Math.ceil(chkIdsGlobal.length / numParts); 
                    const chunks = [];
                    for (let i = 0; i < chkIdsGlobal.length; i += chunkSize) {
                        chunks.push(chkIdsGlobal.slice(i, i + chunkSize));
                    }

                    for (let i = 0; i < chunks.length; i++) {
                        let currentFrame = null;
                        for (const frame of page.frames()) {
                            const isAlive = await frame.evaluate((bId) => !!document.getElementById(bId + '_btn'), baseIdGlobal).catch(()=>false);
                            if (isAlive) { currentFrame = frame; break; }
                        }

                        if (!currentFrame) continue;

                        await currentFrame.evaluate(async (bId, chunkIds) => {
                            const pause = (ms) => new Promise(res => setTimeout(res, ms));
                            const btnOpen = document.getElementById(bId + '_btn');
                            if (btnOpen) btnOpen.click();
                            await pause(1000);
                            const btnClear = document.getElementById('btnClearAll' + bId);
                            if (btnClear) btnClear.click();
                            else if (typeof DDChLSA === 'function') DDChLSA('divList' + bId, false, 0, 1);
                            await pause(1000);
                            
                            for (let id of chunkIds) {
                                const chk = document.getElementById(id);
                                if (chk && !chk.checked) {
                                    chk.click(); 
                                    chk.dispatchEvent(new Event('change', { bubbles: true }));
                                }
                            }
                            await pause(1000);
                            if (btnOpen) btnOpen.click();
                            await pause(1000);
                        }, baseIdGlobal, chunks[i]);
                        
                        await currentFrame.evaluate(() => {
                            const btn = document.querySelector('.ExpBtn') || document.querySelector('a[id*="btnExpR"]');
                            if (btn) btn.click();
                        });

                        let filePath;
                        const start = Date.now();
                        while (Date.now() - start < 180000) { 
                            const files = fs.readdirSync(downloadPath);
                            const finalFile = files.find(f => !f.endsWith('.crdownload') && !f.endsWith('.tmp') && !f.endsWith('.png'));
                            if (finalFile) {
                                const fullPath = path.join(downloadPath, finalFile);
                                if (fs.statSync(fullPath).size > 100) {
                                    await delay(4000); 
                                    filePath = fullPath;
                                    break;
                                }
                            }
                            await delay(4000);
                        }

                        if (!filePath) {
                            log.error(`Timeout en el Parte ${i+1}. Omitiendo...`);
                            continue;
                        }

                        try {
                            const zip = new AdmZip(filePath);
                            const zipEntries = zip.getEntries();
                            let tempRows = []; 

                            const excelEntry = zipEntries.find(e => !e.isDirectory && (e.entryName.toLowerCase().endsWith('.xlsx') || e.entryName.toLowerCase().endsWith('.xls') || e.entryName.toLowerCase().endsWith('.csv')));
                            
                            if (excelEntry) {
                                let workbook = xlsx.read(excelEntry.getData(), { type: 'buffer', cellDates: true });
                                let sheetName = workbook.SheetNames[0];
                                let sheet = workbook.Sheets[sheetName];
                                const range = xlsx.utils.decode_range(sheet['!ref']);
                                
                                let headerRowIdx = range.s.r;
                                for(let R = range.s.r; R <= range.e.r; ++R) {
                                    let foundHeader = false;
                                    for(let C = range.s.c; C <= range.e.c; ++C) {
                                        let cell = sheet[xlsx.utils.encode_cell({c:C, r:R})];
                                        let val = cell ? String(cell.v).trim().toLowerCase() : "";
                                        if (['representante', 'código de pdv', 'id tienda', 'taskid', 'activityid', 'división', 'área'].includes(val)) {
                                            headerRowIdx = R; foundHeader = true; break;
                                        }
                                    }
                                    if(foundHeader) break;
                                }

                                let headers = []; let photoHeaders = []; let normalHeaders = [];
                                for(let C = range.s.c; C <= range.e.c; ++C) {
                                    let cell = sheet[xlsx.utils.encode_cell({c:C, r:headerRowIdx})];
                                    let headerName = cell ? String(cell.v).trim() : `Columna_${C}`;
                                    headers[C] = headerName;
                                    if (headerName.toLowerCase().includes('foto') && headerName.toLowerCase() !== 'fotos') {
                                        photoHeaders.push(C);
                                    } else {
                                        normalHeaders.push(C);
                                    }
                                }

                                for(let R = headerRowIdx + 1; R <= range.e.r; ++R) {
                                    let isEmptyRow = true;
                                    let baseRow = {};
                                    for(let C of normalHeaders) {
                                        let cell = sheet[xlsx.utils.encode_cell({c:C, r:R})];
                                        let val = cell ? (cell.w !== undefined ? cell.w : cell.v) : "";
                                        if (cell && cell.v instanceof Date) {
                                            val = cell.v.toISOString().replace('T', ' ').substring(0, 19); 
                                        }
                                        if (val !== "") isEmptyRow = false;
                                        baseRow[headers[C]] = val;
                                    }
                                    if (isEmptyRow) continue; 

                                    // PROTECCIÓN DE FECHAS ESTRICTA
                                    let fechaRaw = baseRow['fecha'] || baseRow['Fecha'] || baseRow['Fecha de realización'];
                                    let fechaLimpiaStr = fechaReporteFinal; 
                                    if (fechaRaw) {
                                        if (typeof fechaRaw === 'string') {
                                            let soloFecha = fechaRaw.split(' ')[0]; 
                                            if (soloFecha.includes('/')) {
                                                let partes = soloFecha.split('/');
                                                if (partes[2] && partes[2].length === 4) { 
                                                    fechaLimpiaStr = `${partes[2]}-${partes[1].padStart(2, '0')}-${partes[0].padStart(2, '0')}`;
                                                } else if (partes[0] && partes[0].length === 4) { 
                                                    fechaLimpiaStr = `${partes[0]}-${partes[1].padStart(2, '0')}-${partes[2].padStart(2, '0')}`;
                                                }
                                            } else if (soloFecha.includes('-')) {
                                                fechaLimpiaStr = soloFecha; 
                                            }
                                        } else if (fechaRaw instanceof Date) {
                                            fechaLimpiaStr = fechaRaw.toISOString().split('T')[0];
                                        } else if (typeof fechaRaw === 'number') {
                                            let dObj = new Date(Math.round((fechaRaw - 25569) * 864e5));
                                            fechaLimpiaStr = dObj.toISOString().split('T')[0];
                                        }
                                    }
                                    if (!fechaLimpiaStr || fechaLimpiaStr.length < 10) fechaLimpiaStr = fechaReporteFinal;
                                    baseRow['Fecha de realización'] = fechaLimpiaStr; 

                                    let pdvRaw = baseRow['ID Tienda'] || baseRow['Código de PDV'] || 'ND';
                                    let productoRaw = baseRow['Nombre del producto'] || baseRow['Nombre completo del producto'] || 'ND';
                                    let representanteRaw = baseRow['Representante'];
                                    
                                    if (!representanteRaw || String(representanteRaw).trim() === '') {
                                        if (photoHeaders.length > 0) {
                                            let cell = sheet[xlsx.utils.encode_cell({c:photoHeaders[0], r:R})];
                                            let firstPhotoLink = (cell && cell.l && cell.l.Target) ? cell.l.Target : (cell ? String(cell.v) : "");
                                            representanteRaw = extraerRepresentante(firstPhotoLink, fechaLimpiaStr);
                                        } else {
                                            representanteRaw = 'DESCONOCIDO';
                                        }
                                    } else {
                                        representanteRaw = String(representanteRaw).toUpperCase();
                                    }
                                    baseRow['Representante'] = representanteRaw;

                                    let fechaLimpia = limpiarTextoParaArchivo(fechaLimpiaStr, 15);
                                    let pdvLimpio = limpiarTextoParaArchivo(pdvRaw, 30);
                                    let productoLimpio = limpiarTextoParaArchivo(productoRaw, 100); 
                                    let representanteLimpio = limpiarTextoParaArchivo(representanteRaw, 50);
                                    let baseNameData = `${fechaLimpia}_${pdvLimpio}_${productoLimpio}_${representanteLimpio}`;

                                    let fotosEnFilaTemp = [];
                                    for(let C of photoHeaders) {
                                        let header = headers[C];
                                        let cell = sheet[xlsx.utils.encode_cell({c:C, r:R})];
                                        let linkVal = "";
                                        
                                        if (cell && cell.l && cell.l.Target) linkVal = cell.l.Target;
                                        else if (cell && cell.f) {
                                            let m = cell.f.match(/"([^"]+\.jpg|[^"]+\.png|[^"]+\.jpeg)"/i);
                                            if (m) linkVal = m[1];
                                        } else if (cell && cell.v && (String(cell.v).includes('http') || String(cell.v).includes('files'))) {
                                            linkVal = String(cell.v);
                                        }
                                        
                                        if (linkVal && (linkVal.toLowerCase().includes('.jpg') || linkVal.toLowerCase().includes('.png') || linkVal.toLowerCase().includes('.jpeg'))) {
                                            let originalBaseName = linkVal.split('\\').pop().split('/').pop();
                                            let tipoFotoLimpio = limpiarTextoParaArchivo(header, 30); 
                                            let ext = path.extname(originalBaseName) || '.jpg';
                                            if (!ext.includes('.')) ext = '.jpg';
                                            let uniqueImageName = `${tipoFotoLimpio}_${baseNameData}${ext}`;
                                            fotosEnFilaTemp.push({ tipo: header, uniqueImageName: uniqueImageName, urlVieja: linkVal, originalBaseName: originalBaseName });
                                        }
                                    }
                                    tempRows.push({ unidad: unidadNegocioActual, baseRow: baseRow, fotos: fotosEnFilaTemp });
                                }
                            }

                            const fotosAEnviarZip = zipEntries.filter(e => !e.isDirectory && (e.entryName.toLowerCase().endsWith('.jpg') || e.entryName.toLowerCase().endsWith('.png') || e.entryName.toLowerCase().endsWith('.jpeg')));
                            let zipPhotosMap = {};
                            fotosAEnviarZip.forEach(e => {
                                zipPhotosMap[normalizarKey(path.basename(e.entryName))] = e.getData();
                            });

                            let dictAzureLinks = {}; 
                            let promesasSubida = [];

                            for (let temp of tempRows) {
                                for (let fotoObj of temp.fotos) {
                                    promesasSubida.push(async () => {
                                        let finalImageName = fotoObj.uniqueImageName;
                                        const blobName = `${rutaCarpetaVirtual}/${finalImageName}`;
                                        
                                        // 🚀 AQUÍ APLICAMOS EL CLIENTE AC CORRECTO
                                        const blockBlobClient = containerClientAc.getBlockBlobClient(blobName);
                                        
                                        // 🚀 TURBINA 2: IDEMPOTENCIA CLOUD (Omite fotos ya rescatadas)
                                        const exists = await blockBlobClient.exists().catch(()=>false);
                                        if (exists) {
                                            dictAzureLinks[finalImageName] = blockBlobClient.url;
                                            return; 
                                        } 
                                        
                                        let bufferData = zipPhotosMap[normalizarKey(fotoObj.originalBaseName)];
                                        if (!bufferData) {
                                            bufferData = await descargarFoto(fotoObj.urlVieja); 
                                        }

                                        if (bufferData) {
                                            // 🚀 AQUI APLICAMOS LA FUNCIÓN SUBIRAAZURE ACTUALIZADA AL CONTENEDOR AC
                                            let link = await subirAAzure(finalImageName, bufferData, rutaCarpetaVirtual, containerClientAc);
                                            if (link) dictAzureLinks[finalImageName] = link;
                                        }
                                    });
                                }
                            }

                            if (promesasSubida.length > 0) {
                                log.info(`Subiendo lote de ${promesasSubida.length} fotos a Azure...`);
                                let contadorSubidas = 0;
                                const PARALLEL_LIMIT = 50; 
                                for (let i = 0; i < promesasSubida.length; i += PARALLEL_LIMIT) {
                                    const lote = promesasSubida.slice(i, i + PARALLEL_LIMIT).map(fn => fn());
                                    await Promise.all(lote);
                                    contadorSubidas += lote.length;
                                }
                            }

                            // 🚀 TURBINA 4: LIBERACIÓN ACTIVA DE MEMORIA (Anticrash)
                            zipPhotosMap = null; 
                            
                            // ENSAMBLE
                            for (let temp of tempRows) {
                                if (temp.fotos.length > 0) {
                                    for (let fotoObj of temp.fotos) {
                                        let linkDirecto = dictAzureLinks[fotoObj.uniqueImageName] || "Error/Sin subir";
                                        masterExcelData.push({ 'UnidadNegocios': temp.unidad, ...temp.baseRow, 'Tipo de Foto': fotoObj.tipo, 'Fotos': linkDirecto });
                                    }
                                } else {
                                    masterExcelData.push({ 'UnidadNegocios': temp.unidad, ...temp.baseRow, 'Tipo de Foto': "Sin Foto", 'Fotos': "" });
                                }
                            }

                        } catch (errorZip) {
                            log.error(`Error en Parseo Zip: ${errorZip.message}`);
                        }
                        
                        // DESTRUCCIÓN INMEDIATA DEL ZIP FÍSICO
                        fs.unlinkSync(filePath); 
                    } 
                } catch (errorNavegacion) {
                    log.error(`Fallo general navegando en ${nombreReporte}: ${errorNavegacion.message}`);
                }
                
                await page.close(); 
            } 

            // ==============================================================================
            // FASE 2: REPORTES AC CONTROL DINÁMICA COMERCIAL (USA CONTENEDOR AC Y NOMENCLATURA NUEVA)
            // ==============================================================================
            log.info(`=========================================================`);
            log.success(`🚀 INICIANDO FASE 2: REPORTES AC CONTROL DINÁMICA COMERCIAL`);
            log.info(`=========================================================`);

            const REPORTES_FASE_2 = [
                "AC Control Dinámica Comercial Alimentos",
                "AC Control Dinámica Comercial Coasis",
                "AC Control Dinámica Comercial P&G",
                "AC Control Dinámica Comercial Snacks"
            ];

            const UNIDADES_FASE_2 = {
                "AC Control Dinámica Comercial Alimentos": "Alimentos",
                "AC Control Dinámica Comercial Coasis": "Coasis",
                "AC Control Dinámica Comercial P&G": "CPH",
                "AC Control Dinámica Comercial Snacks": "Snacks"
            };

            for (const nombreReporte of REPORTES_FASE_2) {
                const unidadNegocioActual = UNIDADES_FASE_2[nombreReporte] || "General";
                log.info(`>>> PROCESANDO REPORTE FASE 2: ${nombreReporte} (${unidadNegocioActual})`);

                const page = await browser.newPage();
                await page.setViewport({ width: 1920, height: 1080 });
                await page.evaluateOnNewDocument(() => { window.name = '_eld_'; });
                page.setDefaultNavigationTimeout(240000); 
                page.setDefaultTimeout(240000);

                const browserSession = await page.target().createCDPSession();
                await browserSession.send('Browser.setDownloadBehavior', { behavior: 'allow', downloadPath: downloadPath, eventsEnabled: true });

                try {
                    log.info('Paso 1: Login (Fase 2)...');
                    await page.goto('https://mob.eleader.biz/mob2301/SysLoginAjax.aspx', { waitUntil: 'networkidle2' });
                    await delay(3000);
                    
                    const txtUserExists = await page.$('#txtUser');
                    if (txtUserExists) {
                        await page.type('#txtUser', process.env.ELEADER_USER || '', { delay: 50 });
                        await page.type('#txtFirm', process.env.ELEADER_COMPANY || '', { delay: 50 });
                        await page.type('#txtPassword', process.env.ELEADER_PASS || '', { delay: 50 });
                        await Promise.all([
                            page.keyboard.press('Enter'),
                            page.waitForNavigation({ waitUntil: 'networkidle2' }).catch(() => {}) 
                        ]);
                    }

                    log.info('Paso 2: Navegando al Dashboard (Fase 2)...');
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
                    await delay(10000); 

                    await page.evaluate(() => {
                        const elements = Array.from(document.querySelectorAll('a, span, div, li'));
                        const tareas = elements.find(el => el.textContent.trim() === 'Informes de tareas');
                        if (tareas) tareas.click();
                    });
                    await delay(6000); 

                    log.info(`Paso 3: Buscando el reporte: ${nombreReporte}...`);
                    const searchInputSelector = 'input[id*="srch"], input[placeholder*="ntroduce"]';
                    try {
                        await page.waitForSelector(searchInputSelector, { timeout: 10000 });
                        await page.focus(searchInputSelector);
                        await page.click(searchInputSelector, { clickCount: 3 });
                        await page.keyboard.press('Backspace');
                        await page.type(searchInputSelector, nombreReporte, { delay: 100 });
                        await delay(1000);
                        await page.keyboard.press('Enter');
                        await delay(8000); 
                    } catch (err) {}

                    const reportClicked = await page.evaluate((targetName) => {
                        const links = Array.from(document.querySelectorAll('a, span, td'));
                        const target = links.find(el => el.textContent.toLowerCase().replace(/\s+/g, ' ').trim().includes(targetName.toLowerCase()));
                        if (target) { target.click(); return true; }
                        return false;
                    }, nombreReporte);

                    if (!reportClicked) {
                        log.warn(`[OMITIDO] No se encontró en pantalla el reporte ${nombreReporte}.`);
                        await page.close(); 
                        continue;
                    }

                    await delay(2000); 
                    await page.evaluate(() => {
                        const elements = Array.from(document.querySelectorAll('a, span, div, button, input'));
                        const btn = elements.find(el => (el.textContent || el.value || '').toLowerCase().includes('pasar a informe'));
                        if (btn) btn.click();
                    });
                    
                    log.info('Paso 4: Entorno de filtros cargando (Fase 2)...');
                    await delay(15000); 

                    log.info(`Paso 5: Forzando Fecha Estricta...`);
                    for (const frame of page.frames()) {
                        try {
                            await frame.evaluate((yVal, mVal, dVal) => {
                                const inputsY = document.querySelectorAll('input[placeholder="AAAA"], input[placeholder="YYYY"], input[placeholder="yyyy"]');
                                const inputsM = document.querySelectorAll('input[placeholder="MM"], input[placeholder="mm"]');
                                const inputsD = document.querySelectorAll('input[placeholder="DD"], input[placeholder="dd"]');
                                
                                for (let k = 0; k < inputsY.length; k++) {
                                    if (inputsY[k]) { inputsY[k].value = yVal; inputsY[k].dispatchEvent(new Event('input', {bubbles:true})); }
                                    if (inputsM[k]) { inputsM[k].value = mVal; inputsM[k].dispatchEvent(new Event('input', {bubbles:true})); }
                                    if (inputsD[k]) { 
                                        inputsD[k].value = dVal; 
                                        inputsD[k].dispatchEvent(new Event('input', {bubbles:true})); 
                                        inputsD[k].dispatchEvent(new Event('change', {bubbles:true})); 
                                        inputsD[k].dispatchEvent(new Event('blur', {bubbles:true})); 
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
                        try {
                            const col1InputSelector = 'input[name$="$acCol1"]';
                            const inputExists = await frame.$(col1InputSelector);
                            if (inputExists) {
                                await frame.evaluate((selector) => { document.querySelector(selector).value = ''; }, col1InputSelector);
                                await inputExists.type('Nombre del objeto', { delay: 50 });
                                await delay(1000);
                                await inputExists.press('Enter');
                            }
                        } catch (err) {}
                    }
                    await delay(3000);

                    log.info('Paso 6: Mapeando Representantes (Fase 2)...');
                    let activeFrame = null;
                    for (const frame of page.frames()) {
                        const clicked = await frame.evaluate(() => {
                            const tds = Array.from(document.querySelectorAll('td'));
                            const lbl = tds.find(td => td.textContent.trim() === 'Representante:');
                            if (lbl && lbl.nextElementSibling) {
                                const btn = lbl.nextElementSibling.querySelector('input[type="button"], .DDBtn, img');
                                if (btn) { btn.click(); return true; }
                            }
                            return false;
                        }).catch(() => false);

                        if (clicked) { activeFrame = frame; break; }
                    }
                    await delay(4000); 

                    const mapData = await activeFrame.evaluate(() => {
                        const chks = Array.from(document.querySelectorAll('input[type="checkbox"][id*="innerRealExecutor"]'));
                        if (chks.length === 0) return null;
                        const firstId = chks[0].id;
                        const baseId = firstId.substring(0, firstId.lastIndexOf('_')); 
                        return { baseId: baseId, ids: chks.map(c => c.id) };
                    });

                    if(!mapData) {
                        log.warn(`[VACÍO] No se encontraron representantes.`);
                        await page.close();
                        continue;
                    }

                    let baseIdGlobal = mapData.baseId;
                    let chkIdsGlobal = mapData.ids;
                    const numParts = 4;
                    const chunkSize = Math.ceil(chkIdsGlobal.length / numParts); 
                    const chunks = [];
                    for (let i = 0; i < chkIdsGlobal.length; i += chunkSize) {
                        chunks.push(chkIdsGlobal.slice(i, i + chunkSize));
                    }

                    for (let i = 0; i < chunks.length; i++) {
                        const nombreParte = `${nombreReporte}_Parte${i + 1}`;
                        log.info(`\n--- DESCARGANDO Y EXPORTANDO FASE 2: ${nombreParte} ---`);

                        let currentFrame = null;
                        for (const frame of page.frames()) {
                            const isAlive = await frame.evaluate((bId) => !!document.getElementById(bId + '_btn'), baseIdGlobal).catch(()=>false);
                            if (isAlive) { currentFrame = frame; break; }
                        }

                        if (!currentFrame) continue;

                        await currentFrame.evaluate(async (bId, chunkIds) => {
                            const pause = (ms) => new Promise(res => setTimeout(res, ms));
                            const btnOpen = document.getElementById(bId + '_btn');
                            if (btnOpen) btnOpen.click();
                            await pause(1500);
                            const btnClear = document.getElementById('btnClearAll' + bId);
                            if (btnClear) btnClear.click();
                            else if (typeof DDChLSA === 'function') DDChLSA('divList' + bId, false, 0, 1);
                            await pause(1500);
                            
                            for (let id of chunkIds) {
                                const chk = document.getElementById(id);
                                if (chk && !chk.checked) {
                                    chk.click(); 
                                    chk.dispatchEvent(new Event('change', { bubbles: true }));
                                    await pause(30);
                                }
                            }
                            await pause(1500);
                            if (btnOpen) btnOpen.click();
                            await pause(1500);
                        }, baseIdGlobal, chunks[i]);

                        await currentFrame.evaluate(() => {
                            const btn = document.querySelector('.ExpBtn') || document.querySelector('a[id*="btnExpR"]');
                            if (btn) btn.click();
                        });

                        let filePath;
                        const start = Date.now();
                        while (Date.now() - start < 180000) {
                            const files = fs.readdirSync(downloadPath);
                            const finalFile = files.find(f => !f.endsWith('.crdownload') && !f.endsWith('.tmp') && !f.endsWith('.png'));
                            if (finalFile) {
                                const fullPath = path.join(downloadPath, finalFile);
                                if (fs.statSync(fullPath).size > 100) {
                                    await delay(4000); 
                                    filePath = fullPath;
                                    break;
                                }
                            }
                            await delay(5000);
                        }

                        if (!filePath) continue;

                        try {
                            const zip = new AdmZip(filePath);
                            const zipEntries = zip.getEntries();
                            let tempRows = []; 

                            const excelEntry = zipEntries.find(e => !e.isDirectory && (e.entryName.toLowerCase().endsWith('.xlsx') || e.entryName.toLowerCase().endsWith('.xls') || e.entryName.toLowerCase().endsWith('.csv')));
                            
                            if (excelEntry) {
                                let workbook = xlsx.read(excelEntry.getData(), { type: 'buffer', cellDates: true });
                                let sheetName = workbook.SheetNames[0];
                                let sheet = workbook.Sheets[sheetName];
                                const range = xlsx.utils.decode_range(sheet['!ref']);
                                
                                let headerRowIdx = range.s.r;
                                for(let R = range.s.r; R <= range.e.r; ++R) {
                                    let foundHeader = false;
                                    for(let C = range.s.c; C <= range.e.c; ++C) {
                                        let cell = sheet[xlsx.utils.encode_cell({c:C, r:R})];
                                        let val = cell ? String(cell.v).trim().toLowerCase() : "";
                                        if (['representante', 'código de pdv', 'id tienda', 'taskid', 'activityid', 'división', 'área'].includes(val)) {
                                            headerRowIdx = R; foundHeader = true; break;
                                        }
                                    }
                                    if(foundHeader) break;
                                }

                                let headers = []; let photoHeaders = []; let normalHeaders = [];
                                
                                for(let C = range.s.c; C <= range.e.c; ++C) {
                                    let cell = sheet[xlsx.utils.encode_cell({c:C, r:headerRowIdx})];
                                    let headerName = cell ? String(cell.v).trim() : `Columna_${C}`;
                                    headers[C] = headerName;
                                    
                                    if (headerName.toLowerCase().includes('foto') && headerName.toLowerCase() !== 'fotos') {
                                        photoHeaders.push(C);
                                    } else {
                                        normalHeaders.push(C);
                                    }
                                }

                                for(let R = headerRowIdx + 1; R <= range.e.r; ++R) {
                                    let isEmptyRow = true;
                                    let baseRow = {};
                                    for(let C of normalHeaders) {
                                        let cell = sheet[xlsx.utils.encode_cell({c:C, r:R})];
                                        let val = cell ? (cell.w !== undefined ? cell.w : cell.v) : "";
                                        if (cell && cell.v instanceof Date) val = cell.v.toISOString().split('T')[0];
                                        if (val !== "") isEmptyRow = false;
                                        baseRow[headers[C]] = val;
                                    }
                                    if (isEmptyRow) continue; 

                                    let fechaRaw = baseRow['fecha'] || baseRow['Fecha'] || baseRow['Fecha de realización'];
                                    let dObj;
                                    if (fechaRaw instanceof Date) { dObj = fechaRaw; } 
                                    else if (typeof fechaRaw === 'number') { dObj = new Date(Math.round((fechaRaw - 25569) * 864e5)); } 
                                    else if (typeof fechaRaw === 'string') {
                                        if (fechaRaw.includes('/')) {
                                            let partes = fechaRaw.split('/');
                                            if (partes[2].length === 4) dObj = new Date(partes[2], partes[1] - 1, partes[0]);
                                            else dObj = new Date(fechaRaw);
                                        } else dObj = new Date(fechaRaw);
                                    }
                                    if (!dObj || isNaN(dObj.getTime())) dObj = new Date();
                                    
                                    let fechaLimpiaStr = `${dObj.getFullYear().toString()}-${(dObj.getMonth() + 1).toString().padStart(2, '0')}-${dObj.getDate().toString().padStart(2, '0')}`;
                                    baseRow['Fecha de realización'] = fechaLimpiaStr; 

                                    let pdvRaw = baseRow['ID Tienda'] || baseRow['Código de PDV'] || 'ND';
                                    let productoRaw = baseRow['Nombre del producto'] || baseRow['Nombre completo del producto'] || 'ND';
                                    
                                    let representanteRaw = baseRow['Representante'];
                                    if (!representanteRaw || String(representanteRaw).trim() === '') {
                                        if (photoHeaders.length > 0) {
                                            let cell = sheet[xlsx.utils.encode_cell({c:photoHeaders[0], r:R})];
                                            let firstPhotoLink = (cell && cell.l && cell.l.Target) ? cell.l.Target : (cell ? String(cell.v) : "");
                                            representanteRaw = extraerRepresentante(firstPhotoLink, fechaLimpiaStr);
                                        } else {
                                            representanteRaw = 'DESCONOCIDO';
                                        }
                                    } else {
                                        representanteRaw = String(representanteRaw).toUpperCase();
                                    }
                                    baseRow['Representante'] = representanteRaw;

let fechaLimpia = limpiarTextoParaArchivo(fechaLimpiaStr, 15); // <-- ESTA LÍNEA FALTABA
                                    let pdvLimpio = limpiarTextoParaArchivo(pdvRaw, 30);
                                    let productoLimpio = limpiarTextoParaArchivo(productoRaw, 100); 
                                    let representanteLimpio = limpiarTextoParaArchivo(representanteRaw, 50);
                                    
                                    // 🚀 NOMENCLATURA EXACTA FASE 2: Código de PDV_Nombre completo del producto_Representante_Fecha
                                    let baseNameDataF2 = `${pdvLimpio}_${productoLimpio}_${representanteLimpio}_${fechaLimpia}`;

                                    let fotosEnFilaTemp = [];
                                    for(let C of photoHeaders) {
                                        let header = headers[C];
                                        let cell = sheet[xlsx.utils.encode_cell({c:C, r:R})];
                                        
                                        let linkVal = "";
                                        if (cell && cell.l && cell.l.Target) {
                                            linkVal = cell.l.Target;
                                        } else if (cell && cell.f) {
                                            let m = cell.f.match(/"([^"]+\.jpg|[^"]+\.png|[^"]+\.jpeg)"/i);
                                            if (m) linkVal = m[1];
                                        } else if (cell && cell.v && (String(cell.v).includes('http') || String(cell.v).includes('files'))) {
                                            linkVal = String(cell.v);
                                        }
                                        
                                        if (linkVal && (linkVal.toLowerCase().includes('.jpg') || linkVal.toLowerCase().includes('.png') || linkVal.toLowerCase().includes('.jpeg'))) {
                                            let originalBaseName = linkVal.split('\\').pop().split('/').pop();
                                            // La función de limpieza se encargará de remover el '/' de 'Foto / Photo' y convertirlo a 'Foto_Photo' 
                                            let tipoFotoLimpio = limpiarTextoParaArchivo(header, 30); 
                                            let ext = path.extname(originalBaseName) || '.jpg';
                                            if (!ext.includes('.')) ext = '.jpg';
                                            
                                            // 🚀 ARMADO FINAL FASE 2: PDV_Producto_Representante_TipoDeFoto.jpg
                                            let uniqueImageName = `${baseNameDataF2}_${tipoFotoLimpio}${ext}`;
                                            fotosEnFilaTemp.push({ tipo: header, uniqueImageName: uniqueImageName, urlVieja: linkVal, originalBaseName: originalBaseName });
                                        }
                                    }
                                    tempRows.push({ unidad: unidadNegocioActual, baseRow: baseRow, fotos: fotosEnFilaTemp });
                                }
                            }

                            const fotosAEnviarZip = zipEntries.filter(e => !e.isDirectory && (e.entryName.toLowerCase().endsWith('.jpg') || e.entryName.toLowerCase().endsWith('.png') || e.entryName.toLowerCase().endsWith('.jpeg')));
                            let zipPhotosMap = {};
                            fotosAEnviarZip.forEach(e => {
                                let llavePerfecta = normalizarKey(path.basename(e.entryName));
                                zipPhotosMap[llavePerfecta] = e.getData();
                            });

                            let dictAzureLinks = {}; 
                            let promesasSubida = [];

                            for (let temp of tempRows) {
                                for (let fotoObj of temp.fotos) {
                                    promesasSubida.push(async () => {
                                        let finalImageName = fotoObj.uniqueImageName;
                                        let bufferData = zipPhotosMap[normalizarKey(fotoObj.originalBaseName)];
                                        let link = "";
                                        
                                        // 🚀 AQUÍ SE USA EL CONTENEDOR NUEVO: fotos-ac
                                        const blockBlobClient = containerClientAc.getBlockBlobClient(`${rutaCarpetaVirtual}/${finalImageName}`);
                                        const exists = await blockBlobClient.exists().catch(()=>false);
                                        
                                        if (exists) {
                                            link = blockBlobClient.url;
                                        } else {
                                            if (!bufferData) bufferData = await descargarFoto(fotoObj.urlVieja);
                                            if (bufferData) link = await subirAAzure(finalImageName, bufferData, rutaCarpetaVirtual, containerClientAc);
                                        }
                                        if (link) dictAzureLinks[finalImageName] = link;
                                    });
                                }
                            }

                            if (promesasSubida.length > 0) {
                                log.info(`Inyectando ${promesasSubida.length} fotos al contenedor [fotos-ac] (Fase 2)...`);
                                let contadorSubidas = 0;
                                const PARALLEL_LIMIT = 50; 
                                for (let i = 0; i < promesasSubida.length; i += PARALLEL_LIMIT) {
                                    const lote = promesasSubida.slice(i, i + PARALLEL_LIMIT).map(fn => fn());
                                    await Promise.all(lote);
                                    contadorSubidas += lote.length;
                                }
                                log.success(`Proceso Fase 2 completado (${contadorSubidas} fotos subidas a fotos-ac).`);
                            }
                            
                            // 🚀 TURBINA 4: LIBERACIÓN ACTIVA DE MEMORIA (Anticrash)
                            zipPhotosMap = null; 

                            for (let temp of tempRows) {
                                if (temp.fotos.length > 0) {
                                    for (let fotoObj of temp.fotos) {
                                        let linkDirecto = dictAzureLinks[fotoObj.uniqueImageName] || "Error/Sin subir";
                                        masterExcelData.push({ 'UnidadNegocios': temp.unidad, ...temp.baseRow, 'Tipo de Foto': fotoObj.tipo, 'Fotos': linkDirecto });
                                    }
                                } else {
                                    masterExcelData.push({ 'UnidadNegocios': temp.unidad, ...temp.baseRow, 'Tipo de Foto': "Sin Foto", 'Fotos': "" });
                                }
                            }

                        } catch (errorZip) {}
                        fs.unlinkSync(filePath); 
                    } 
                } catch (errorNavegacion) {
                    log.error('Fallo en navegacion F2');
                }
                await page.close(); 
            }

            // ==============================================================================
            // FASE 3: REPORTES PROMOCIONES ACUERDO COMERCIAL (USA CONTENEDOR PROMOCIONES)
            // ==============================================================================
            log.info(`=========================================================`);
            log.success(`🚀 INICIANDO FASE 3: REPORTES PROMOCIONES ACUERDO COMERCIAL`);
            log.info(`=========================================================`);

            const REPORTES_FASE_3 = [
                "Promociones Acuerdo Comercial Alimentos",
                "Promociones Acuerdo Comercial Coasis",
                "Promociones Acuerdo Comercial CPH",
                "Promociones Acuerdo Comercial Snacks"
            ];

            const UNIDADES_FASE_3 = {
                "Promociones Acuerdo Comercial Alimentos": "Alimentos",
                "Promociones Acuerdo Comercial Coasis": "Coasis",
                "Promociones Acuerdo Comercial CPH": "CPH",
                "Promociones Acuerdo Comercial Snacks": "Snacks"
            };

            for (const nombreReporte of REPORTES_FASE_3) {
                const unidadNegocioActual = UNIDADES_FASE_3[nombreReporte] || "General";
                log.info(`>>> PROCESANDO REPORTE FASE 3: ${nombreReporte} (${unidadNegocioActual})`);

                const page = await browser.newPage();
                await page.setViewport({ width: 1920, height: 1080 });
                await page.evaluateOnNewDocument(() => { window.name = '_eld_'; });
                page.setDefaultNavigationTimeout(240000); 
                page.setDefaultTimeout(240000);

                const browserSession = await page.target().createCDPSession();
                await browserSession.send('Browser.setDownloadBehavior', { behavior: 'allow', downloadPath: downloadPath, eventsEnabled: true });

                try {
                    log.info('Paso 1: Login (Fase 3)...');
                    await page.goto('https://mob.eleader.biz/mob2301/SysLoginAjax.aspx', { waitUntil: 'networkidle2' });
                    await delay(3000);
                    
                    const txtUserExists = await page.$('#txtUser');
                    if (txtUserExists) {
                        await page.type('#txtUser', process.env.ELEADER_USER || '', { delay: 50 });
                        await page.type('#txtFirm', process.env.ELEADER_COMPANY || '', { delay: 50 });
                        await page.type('#txtPassword', process.env.ELEADER_PASS || '', { delay: 50 });
                        await Promise.all([
                            page.keyboard.press('Enter'),
                            page.waitForNavigation({ waitUntil: 'networkidle2' }).catch(() => {}) 
                        ]);
                    }

                    log.info('Paso 2: Navegando al Dashboard (Fase 3)...');
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
                    await delay(10000); 

                    await page.evaluate(() => {
                        const elements = Array.from(document.querySelectorAll('a, span, div, li'));
                        const tareas = elements.find(el => el.textContent.trim() === 'Informes de tareas');
                        if (tareas) tareas.click();
                    });
                    await delay(6000); 

                    log.info(`Paso 3: Buscando el reporte: ${nombreReporte}...`);
                    const searchInputSelector = 'input[id*="srch"], input[placeholder*="ntroduce"]';
                    try {
                        await page.waitForSelector(searchInputSelector, { timeout: 10000 });
                        await page.focus(searchInputSelector);
                        await page.click(searchInputSelector, { clickCount: 3 });
                        await page.keyboard.press('Backspace');
                        await page.type(searchInputSelector, nombreReporte, { delay: 100 });
                        await delay(1000);
                        await page.keyboard.press('Enter');
                        await delay(8000); 
                    } catch (err) {}

                    const reportClicked = await page.evaluate((targetName) => {
                        const links = Array.from(document.querySelectorAll('a, span, td'));
                        const target = links.find(el => el.textContent.toLowerCase().replace(/\s+/g, ' ').trim().includes(targetName.toLowerCase()));
                        if (target) { target.click(); return true; }
                        return false;
                    }, nombreReporte);

                    if (!reportClicked) {
                        log.warn(`[OMITIDO] No se encontró en pantalla el reporte ${nombreReporte}.`);
                        await page.close(); 
                        continue;
                    }

                    await delay(2000); 
                    await page.evaluate(() => {
                        const elements = Array.from(document.querySelectorAll('a, span, div, button, input'));
                        const btn = elements.find(el => (el.textContent || el.value || '').toLowerCase().includes('pasar a informe'));
                        if (btn) btn.click();
                    });
                    
                    log.info('Paso 4: Entorno de filtros cargando (Fase 3)...');
                    await delay(15000); 

                    log.info(`Paso 5: Forzando Fecha Estricta...`);
                    for (const frame of page.frames()) {
                        try {
                            await frame.evaluate((yVal, mVal, dVal) => {
                                const inputsY = document.querySelectorAll('input[placeholder="AAAA"], input[placeholder="YYYY"], input[placeholder="yyyy"]');
                                const inputsM = document.querySelectorAll('input[placeholder="MM"], input[placeholder="mm"]');
                                const inputsD = document.querySelectorAll('input[placeholder="DD"], input[placeholder="dd"]');
                                
                                for (let k = 0; k < inputsY.length; k++) {
                                    if (inputsY[k]) { inputsY[k].value = yVal; inputsY[k].dispatchEvent(new Event('input', {bubbles:true})); }
                                    if (inputsM[k]) { inputsM[k].value = mVal; inputsM[k].dispatchEvent(new Event('input', {bubbles:true})); }
                                    if (inputsD[k]) { 
                                        inputsD[k].value = dVal; 
                                        inputsD[k].dispatchEvent(new Event('input', {bubbles:true})); 
                                        inputsD[k].dispatchEvent(new Event('change', {bubbles:true})); 
                                        inputsD[k].dispatchEvent(new Event('blur', {bubbles:true})); 
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
                        try {
                            const col1InputSelector = 'input[name$="$acCol1"]';
                            const inputExists = await frame.$(col1InputSelector);
                            if (inputExists) {
                                await frame.evaluate((selector) => { document.querySelector(selector).value = ''; }, col1InputSelector);
                                await inputExists.type('Nombre del objeto', { delay: 50 });
                                await delay(1000);
                                await inputExists.press('Enter');
                            }
                        } catch (err) {}
                    }
                    await delay(3000);

                    log.info('Paso 6: Mapeando Representantes (Fase 3)...');
                    let activeFrame = null;
                    for (const frame of page.frames()) {
                        const clicked = await frame.evaluate(() => {
                            const tds = Array.from(document.querySelectorAll('td'));
                            const lbl = tds.find(td => td.textContent.trim() === 'Representante:');
                            if (lbl && lbl.nextElementSibling) {
                                const btn = lbl.nextElementSibling.querySelector('input[type="button"], .DDBtn, img');
                                if (btn) { btn.click(); return true; }
                            }
                            return false;
                        }).catch(() => false);

                        if (clicked) { activeFrame = frame; break; }
                    }
                    await delay(4000); 

                    const mapData = await activeFrame.evaluate(() => {
                        const chks = Array.from(document.querySelectorAll('input[type="checkbox"][id*="innerRealExecutor"]'));
                        if (chks.length === 0) return null;
                        const firstId = chks[0].id;
                        const baseId = firstId.substring(0, firstId.lastIndexOf('_')); 
                        return { baseId: baseId, ids: chks.map(c => c.id) };
                    });

                    if(!mapData) {
                        log.warn(`[VACÍO] Reporte de Promociones en blanco. Saltando...`);
                        await page.close();
                        continue;
                    }

                    let baseIdGlobal = mapData.baseId;
                    let chkIdsGlobal = mapData.ids;
                    
                    const numParts = chkIdsGlobal.length > 50 ? 4 : 2; 
                    const chunkSize = Math.ceil(chkIdsGlobal.length / numParts); 
                    const chunks = [];
                    for (let i = 0; i < chkIdsGlobal.length; i += chunkSize) {
                        chunks.push(chkIdsGlobal.slice(i, i + chunkSize));
                    }

                    for (let i = 0; i < chunks.length; i++) {
                        const nombreParte = `${nombreReporte}_Parte${i + 1}`;
                        log.info(`\n--- DESCARGANDO Y EXPORTANDO FASE 3: ${nombreParte} ---`);

                        let currentFrame = null;
                        for (const frame of page.frames()) {
                            const isAlive = await frame.evaluate((bId) => !!document.getElementById(bId + '_btn'), baseIdGlobal).catch(()=>false);
                            if (isAlive) { currentFrame = frame; break; }
                        }

                        if (!currentFrame) continue;

                        await currentFrame.evaluate(async (bId, chunkIds) => {
                            const pause = (ms) => new Promise(res => setTimeout(res, ms));
                            const btnOpen = document.getElementById(bId + '_btn');
                            if (btnOpen) btnOpen.click();
                            await pause(1000);
                            const btnClear = document.getElementById('btnClearAll' + bId);
                            if (btnClear) btnClear.click();
                            else if (typeof DDChLSA === 'function') DDChLSA('divList' + bId, false, 0, 1);
                            await pause(1000);
                            
                            for (let id of chunkIds) {
                                const chk = document.getElementById(id);
                                if (chk && !chk.checked) {
                                    chk.click(); 
                                    chk.dispatchEvent(new Event('change', { bubbles: true }));
                                }
                            }
                            await pause(1000);
                            if (btnOpen) btnOpen.click();
                            await pause(1000);
                        }, baseIdGlobal, chunks[i]);
                        
                        await currentFrame.evaluate(() => {
                            const btn = document.querySelector('.ExpBtn') || document.querySelector('a[id*="btnExpR"]');
                            if (btn) btn.click();
                        });

                        let filePath;
                        const start = Date.now();
                        while (Date.now() - start < 180000) { 
                            const files = fs.readdirSync(downloadPath);
                            const finalFile = files.find(f => !f.endsWith('.crdownload') && !f.endsWith('.tmp') && !f.endsWith('.png'));
                            if (finalFile) {
                                const fullPath = path.join(downloadPath, finalFile);
                                if (fs.statSync(fullPath).size > 100) {
                                    await delay(4000); 
                                    filePath = fullPath;
                                    break;
                                }
                            }
                            await delay(4000);
                        }

                        if (!filePath) {
                            log.error(`Timeout en el Parte ${i+1} de Fase 3. Omitiendo...`);
                            continue;
                        }

                        try {
                            const zip = new AdmZip(filePath);
                            const zipEntries = zip.getEntries();
                            let tempRows = []; 

                            const excelEntry = zipEntries.find(e => !e.isDirectory && (e.entryName.toLowerCase().endsWith('.xlsx') || e.entryName.toLowerCase().endsWith('.xls') || e.entryName.toLowerCase().endsWith('.csv')));
                            
                            if (excelEntry) {
                                let workbook = xlsx.read(excelEntry.getData(), { type: 'buffer', cellDates: true });
                                let sheetName = workbook.SheetNames[0];
                                let sheet = workbook.Sheets[sheetName];
                                const range = xlsx.utils.decode_range(sheet['!ref']);
                                
                                let headerRowIdx = range.s.r;
                                for(let R = range.s.r; R <= range.e.r; ++R) {
                                    let foundHeader = false;
                                    for(let C = range.s.c; C <= range.e.c; ++C) {
                                        let cell = sheet[xlsx.utils.encode_cell({c:C, r:R})];
                                        let val = cell ? String(cell.v).trim().toLowerCase() : "";
                                        if (['representante', 'código de pdv', 'id tienda', 'taskid', 'activityid', 'división', 'área'].includes(val)) {
                                            headerRowIdx = R; foundHeader = true; break;
                                        }
                                    }
                                    if(foundHeader) break;
                                }

                                let headers = []; let photoHeaders = []; let normalHeaders = [];
                                for(let C = range.s.c; C <= range.e.c; ++C) {
                                    let cell = sheet[xlsx.utils.encode_cell({c:C, r:headerRowIdx})];
                                    let headerName = cell ? String(cell.v).trim() : `Columna_${C}`;
                                    headers[C] = headerName;
                                    // 🚀 EL DETECTOR ATRAPA: Foto / Photo, Foto impresora, Foto fleje, y columnas llamadas simplemente "Foto" o " Foto "
                                    if (headerName.toLowerCase().includes('foto') && headerName.toLowerCase() !== 'fotos') {
                                        photoHeaders.push(C);
                                    } else {
                                        normalHeaders.push(C);
                                    }
                                }

                                for(let R = headerRowIdx + 1; R <= range.e.r; ++R) {
                                    let isEmptyRow = true;
                                    let baseRow = {};
                                    for(let C of normalHeaders) {
                                        let cell = sheet[xlsx.utils.encode_cell({c:C, r:R})];
                                        let val = cell ? (cell.w !== undefined ? cell.w : cell.v) : "";
                                        if (cell && cell.v instanceof Date) {
                                            val = cell.v.toISOString().replace('T', ' ').substring(0, 19); 
                                        }
                                        if (val !== "") isEmptyRow = false;
                                        baseRow[headers[C]] = val;
                                    }
                                    if (isEmptyRow) continue; 

                                    // PROTECCIÓN DE FECHAS ESTRICTA
                                    let fechaRaw = baseRow['fecha'] || baseRow['Fecha'] || baseRow['Fecha de realización'];
                                    let fechaLimpiaStr = fechaReporteFinal; 
                                    if (fechaRaw) {
                                        if (typeof fechaRaw === 'string') {
                                            let soloFecha = fechaRaw.split(' ')[0]; 
                                            if (soloFecha.includes('/')) {
                                                let partes = soloFecha.split('/');
                                                if (partes[2] && partes[2].length === 4) { 
                                                    fechaLimpiaStr = `${partes[2]}-${partes[1].padStart(2, '0')}-${partes[0].padStart(2, '0')}`;
                                                } else if (partes[0] && partes[0].length === 4) { 
                                                    fechaLimpiaStr = `${partes[0]}-${partes[1].padStart(2, '0')}-${partes[2].padStart(2, '0')}`;
                                                }
                                            } else if (soloFecha.includes('-')) {
                                                fechaLimpiaStr = soloFecha; 
                                            }
                                        } else if (fechaRaw instanceof Date) {
                                            fechaLimpiaStr = fechaRaw.toISOString().split('T')[0];
                                        } else if (typeof fechaRaw === 'number') {
                                            let dObj = new Date(Math.round((fechaRaw - 25569) * 864e5));
                                            fechaLimpiaStr = dObj.toISOString().split('T')[0];
                                        }
                                    }
                                    if (!fechaLimpiaStr || fechaLimpiaStr.length < 10) fechaLimpiaStr = fechaReporteFinal;
                                    baseRow['Fecha de realización'] = fechaLimpiaStr; 

                                    let pdvRaw = baseRow['ID Tienda'] || baseRow['Código de PDV'] || 'ND';
                                    let productoRaw = baseRow['Nombre del producto'] || baseRow['Nombre completo del producto'] || 'ND';
                                    let representanteRaw = baseRow['Representante'];
                                    
                                    if (!representanteRaw || String(representanteRaw).trim() === '') {
                                        if (photoHeaders.length > 0) {
                                            let cell = sheet[xlsx.utils.encode_cell({c:photoHeaders[0], r:R})];
                                            let firstPhotoLink = (cell && cell.l && cell.l.Target) ? cell.l.Target : (cell ? String(cell.v) : "");
                                            representanteRaw = extraerRepresentante(firstPhotoLink, fechaLimpiaStr);
                                        } else {
                                            representanteRaw = 'DESCONOCIDO';
                                        }
                                    } else {
                                        representanteRaw = String(representanteRaw).toUpperCase();
                                    }
                                    baseRow['Representante'] = representanteRaw;

let fechaLimpia = limpiarTextoParaArchivo(fechaLimpiaStr, 15); // <-- ESTA LÍNEA FALTABA
                                    let pdvLimpio = limpiarTextoParaArchivo(pdvRaw, 30);
                                    let productoLimpio = limpiarTextoParaArchivo(productoRaw, 100); 
                                    let representanteLimpio = limpiarTextoParaArchivo(representanteRaw, 50);
                                    
                                    // 🚀 NOMENCLATURA EXACTA FASE 3: Código de PDV_Nombre completo del producto_Representante_Fecha
                                    let baseNameDataF3 = `${pdvLimpio}_${productoLimpio}_${representanteLimpio}_${fechaLimpia}`;

                                    let fotosEnFilaTemp = [];
                                    for(let C of photoHeaders) {
                                        let header = headers[C];
                                        let cell = sheet[xlsx.utils.encode_cell({c:C, r:R})];
                                        let linkVal = "";
                                        
                                        if (cell && cell.l && cell.l.Target) linkVal = cell.l.Target;
                                        else if (cell && cell.f) {
                                            let m = cell.f.match(/"([^"]+\.jpg|[^"]+\.png|[^"]+\.jpeg)"/i);
                                            if (m) linkVal = m[1];
                                        } else if (cell && cell.v && (String(cell.v).includes('http') || String(cell.v).includes('files'))) {
                                            linkVal = String(cell.v);
                                        }
                                        
                                        if (linkVal && (linkVal.toLowerCase().includes('.jpg') || linkVal.toLowerCase().includes('.png') || linkVal.toLowerCase().includes('.jpeg'))) {
                                            let originalBaseName = linkVal.split('\\').pop().split('/').pop();
                                            // Limpieza convierte mágicamente "Foto fleje" en "Foto_fleje"
                                            let tipoFotoLimpio = limpiarTextoParaArchivo(header, 30); 
                                            let ext = path.extname(originalBaseName) || '.jpg';
                                            if (!ext.includes('.')) ext = '.jpg';
                                            
                                            // 🚀 ARMADO FINAL FASE 3: PDV_Producto_Representante_TipoDeFoto.jpg
                                            let uniqueImageName = `${baseNameDataF3}_${tipoFotoLimpio}${ext}`;
                                            fotosEnFilaTemp.push({ tipo: header, uniqueImageName: uniqueImageName, urlVieja: linkVal, originalBaseName: originalBaseName });
                                        }
                                    }
                                    tempRows.push({ unidad: unidadNegocioActual, baseRow: baseRow, fotos: fotosEnFilaTemp });
                                }
                            }

                            const fotosAEnviarZip = zipEntries.filter(e => !e.isDirectory && (e.entryName.toLowerCase().endsWith('.jpg') || e.entryName.toLowerCase().endsWith('.png') || e.entryName.toLowerCase().endsWith('.jpeg')));
                            let zipPhotosMap = {};
                            fotosAEnviarZip.forEach(e => {
                                zipPhotosMap[normalizarKey(path.basename(e.entryName))] = e.getData();
                            });

                            let dictAzureLinks = {}; 
                            let promesasSubida = [];

                            for (let temp of tempRows) {
                                for (let fotoObj of temp.fotos) {
                                    promesasSubida.push(async () => {
                                        let finalImageName = fotoObj.uniqueImageName;
                                        const blobName = `${rutaCarpetaVirtual}/${finalImageName}`;
                                        
                                        // 🚀 AQUÍ APLICAMOS EL CILIENTE PROMO
                                        const blockBlobClient = containerClientPromo.getBlockBlobClient(blobName);
                                        
                                        const exists = await blockBlobClient.exists().catch(()=>false);
                                        if (exists) {
                                            dictAzureLinks[finalImageName] = blockBlobClient.url;
                                            return; 
                                        } 
                                        
                                        let bufferData = zipPhotosMap[normalizarKey(fotoObj.originalBaseName)];
                                        if (!bufferData) {
                                            bufferData = await descargarFoto(fotoObj.urlVieja); 
                                        }

                                        if (bufferData) {
                                            // 🚀 SE ENVÍA AL CONTENEDOR FOTOS-PROMO
                                            let link = await subirAAzure(finalImageName, bufferData, rutaCarpetaVirtual, containerClientPromo);
                                            if (link) dictAzureLinks[finalImageName] = link;
                                        }
                                    });
                                }
                            }

                            if (promesasSubida.length > 0) {
                                log.info(`Subiendo lote de ${promesasSubida.length} fotos al contenedor [fotos-promo]...`);
                                let contadorSubidas = 0;
                                const PARALLEL_LIMIT = 50; 
                                for (let i = 0; i < promesasSubida.length; i += PARALLEL_LIMIT) {
                                    const lote = promesasSubida.slice(i, i + PARALLEL_LIMIT).map(fn => fn());
                                    await Promise.all(lote);
                                    contadorSubidas += lote.length;
                                }
                                log.success(`Proceso Fase 3 completado (${contadorSubidas} fotos subidas a fotos-promo).`);
                            }

                            zipPhotosMap = null; 
                            
                            // ENSAMBLE AL MASTER
                            for (let temp of tempRows) {
                                if (temp.fotos.length > 0) {
                                    for (let fotoObj of temp.fotos) {
                                        let linkDirecto = dictAzureLinks[fotoObj.uniqueImageName] || "Error/Sin subir";
                                        masterExcelData.push({ 'UnidadNegocios': temp.unidad, ...temp.baseRow, 'Tipo de Foto': fotoObj.tipo, 'Fotos': linkDirecto });
                                    }
                                } else {
                                    masterExcelData.push({ 'UnidadNegocios': temp.unidad, ...temp.baseRow, 'Tipo de Foto': "Sin Foto", 'Fotos': "" });
                                }
                            }

                        } catch (errorZip) {
                            log.error(`Error en Parseo Zip Fase 3: ${errorZip.message}`);
                        }
                        
                        fs.unlinkSync(filePath); 
                    } 
                } catch (errorNavegacion) {
                    log.error(`Fallo general navegando en Promociones: ${errorNavegacion.message}`);
                }
                
                await page.close(); 
            }
            
        } finally {
            await browser.close(); 
        }

        // =========================================================
        // REESCRITURA TOTAL DEL MASTER EXCEL EN AZURE POR DÍA
        // =========================================================
        if (masterExcelData.length > 0) {
            try {
                let newWb = xlsx.utils.book_new();
                let newWs = xlsx.utils.json_to_sheet(masterExcelData);
                xlsx.utils.book_append_sheet(newWb, newWs, "Reporte Consolidado");
                let excelBuffer = xlsx.write(newWb, { type: 'buffer', bookType: 'xlsx' });
                
                const masterFileName = `Master_${fechaReporteFinal}.xlsx`;
                
                const blobName = `EXCEL_DIARIO/${masterFileName}`;
                // EL EXCEL SE GUARDA EN containerClientOsa PARA NO ROMPER POWER BI
                const blockBlobClient = containerClientOsa.getBlockBlobClient(blobName);
                await blockBlobClient.uploadData(excelBuffer, { blobHTTPHeaders: { blobContentType: 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet' } });
                
                log.success(`¡Consolidado de ${fechaReporteFinal} cerrado y guardado!`);
            } catch (errMaster) {
                log.error(`Error subiendo el Master Excel: ${errMaster.message}`);
            }
        } else {
            log.warn(`Sin datos para la fecha ${fechaReporteFinal}.`);
        }
    }

    log.success(`===================================================`);
    log.success(`✅ EXTRACCIÓN DIARIA FINALIZADA CON ÉXITO`);
    log.success(`===================================================`);
    process.exit(0);

})();
