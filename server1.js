// --- Dependencies ---
const express = require('express');
const cheerio = require('cheerio');
const { GoogleGenerativeAI, HarmCategory, HarmBlockThreshold } = require('@google/generative-ai');
const { URL, URLSearchParams } = require('url');
const { config } = require('dotenv');
const NodeCache = require('node-cache'); // Added for caching
const pino = require('pino'); // Added for better logging
const compression = require('compression'); // Added for response compression
const helmet = require('helmet'); // Added for security headers
const cluster = require('cluster'); // Added for multi-core processing
const os = require('os');

config(); // Load environment variables from .env file

// --- Configuration ---
const INITIAL_PAGE_URL = 'https://www.sci.gov.in/case-status-diary-no/';
const AJAX_URL_BASE = 'https://www.sci.gov.in/wp-admin/admin-ajax.php';
const CAPTCHA_IMAGE_BASE_URL = 'https://www.sci.gov.in/';

const GEMINI_API_KEY = process.env.GEMINI_API_KEY;
const GEMINI_MODEL_NAME = process.env.GEMINI_MODEL_NAME || "gemini-2.0-flash";
const MAX_ATTEMPTS = parseInt(process.env.MAX_ATTEMPTS || '3', 10);
const CACHE_TTL = parseInt(process.env.CACHE_TTL || '3600', 10); // Cache TTL in seconds (1 hour default)
const ENABLE_CLUSTERING = process.env.ENABLE_CLUSTERING === 'true';
const REQUEST_TIMEOUT = parseInt(process.env.REQUEST_TIMEOUT || '30000', 10); // 30 seconds default

// Initialize cache
const responseCache = new NodeCache({ 
  stdTTL: CACHE_TTL,
  checkperiod: CACHE_TTL * 0.2,
  useClones: false
});

// Initialize logger
const logger = pino({
  level: process.env.LOG_LEVEL || 'info',
  timestamp: pino.stdTimeFunctions.isoTime,
  formatters: {
    level: (label) => {
      return { level: label };
    }
  }
});

if (!GEMINI_API_KEY) {
    logger.fatal("CRITICAL_ERROR: GEMINI_API_KEY environment variable is not set. API cannot function.");
    process.exit(1);
}

// Initialize Gemini AI
const genAI = new GoogleGenerativeAI(GEMINI_API_KEY);
const model = genAI.getGenerativeModel({
    model: GEMINI_MODEL_NAME,
    safetySettings: [
        { category: HarmCategory.HARM_CATEGORY_HARASSMENT, threshold: HarmBlockThreshold.BLOCK_NONE },
        { category: HarmCategory.HARM_CATEGORY_HATE_SPEECH, threshold: HarmBlockThreshold.BLOCK_NONE },
        { category: HarmCategory.HARM_CATEGORY_SEXUALLY_EXPLICIT, threshold: HarmBlockThreshold.BLOCK_NONE },
        { category: HarmCategory.HARM_CATEGORY_DANGEROUS_CONTENT, threshold: HarmBlockThreshold.BLOCK_NONE },
    ],
});

const COMMON_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36',
    'Accept-Language': 'en-US,en;q=0.9',
};

// --- Core Logic Functions ---

/**
 * Fetches HTML with timeout and error handling
 */
async function fetchHtml(url, options = {}) {
    logger.debug({ url }, 'Fetching HTML');
    
    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), options.timeout || REQUEST_TIMEOUT);
    
    try {
        const fetchOptions = {
            headers: {
                ...COMMON_HEADERS,
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
                'Cache-Control': 'no-cache',
                'Pragma': 'no-cache',
                'Expires': '0',
                ...options.headers
            },
            signal: controller.signal,
            cache: 'no-store'
        };
        
        const response = await fetch(url, fetchOptions);
        
        if (!response.ok) {
            throw new Error(`HTTP ${response.status} for ${url}`);
        }
        
        return await response.text();
    } catch (error) {
        if (error.name === 'AbortError') {
            throw new Error(`Request timeout after ${REQUEST_TIMEOUT}ms for ${url}`);
        }
        throw error;
    } finally {
        clearTimeout(timeoutId);
    }
}

/**
 * Fetches image buffer with timeout and error handling
 */
async function fetchImageBuffer(url) {
    logger.debug({ url }, 'Fetching image');
    
    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), REQUEST_TIMEOUT);
    
    try {
        const fetchOptions = {
            headers: {
                ...COMMON_HEADERS,
                'Accept': 'image/avif,image/webp,image/apng,image/svg+xml,image/*,*/*;q=0.8',
                'Cache-Control': 'no-cache',
                'Pragma': 'no-cache',
                'Expires': '0',
            },
            signal: controller.signal,
            cache: 'no-store'
        };
        
        const response = await fetch(url, fetchOptions);
        
        if (!response.ok) {
            throw new Error(`HTTP ${response.status} for image ${url}`);
        }
        
        const arrayBuffer = await response.arrayBuffer();
        return Buffer.from(arrayBuffer);
    } catch (error) {
        if (error.name === 'AbortError') {
            throw new Error(`Image request timeout after ${REQUEST_TIMEOUT}ms for ${url}`);
        }
        throw error;
    } finally {
        clearTimeout(timeoutId);
    }
}

/**
 * Extracts CSRF token from HTML
 */
function extractToken(html) {
    const $ = cheerio.load(html);
    const tokenInput = $('input[type="hidden"][name^="tok_"]');
    
    if (tokenInput.length > 0) {
        const tokenName = tokenInput.first().attr('name');
        const tokenValue = tokenInput.first().attr('value');
        
        if (tokenName && tokenValue) {
            return { name: tokenName, value: tokenValue };
        }
    }
    
    throw new Error('CSRF token not found in page.');
}

/**
 * Extracts SCID from HTML
 */
function extractSCID(html) {
    const $ = cheerio.load(html);
    const scidInput = $('input#input_siwp_captcha_id_0[name="scid"]');
    
    if (scidInput.length === 0) {
        throw new Error('SCID input field not found.');
    }
    
    const scid = scidInput.val();
    if (!scid) {
        throw new Error('SCID value is empty.');
    }
    
    return scid;
}

/**
 * Solves CAPTCHA using Gemini AI
 */
async function solveCaptchaWithGemini(imageBuffer, requestIdentifier) {
    try {
        logger.debug({ requestId: requestIdentifier }, 'Calling Gemini to solve CAPTCHA');
        
        const result = await model.generateContent([
            `You are given an image of a numerical CAPTCHA that contains a simple arithmetic expression using only addition (+) and subtraction (−). 
            
            1. Extract the expression from the image using OCR.
            2. Solve the expression.
            3. Return ONLY the final numerical result as a string — no extra text, no explanation, no punctuation, and no mention of the expression itself.
            
            Example:  
            Image contains: "8 + 3 " → Output: "11"
            
            DO NOT include any other output except the result.`,
            { inlineData: { data: imageBuffer.toString('base64'), mimeType: 'image/png' } },
        ]);
        
        const response = result.response;
        const text = response.text();
        const cleanedAnswer = text.trim().match(/-?\d+/);
        
        if (cleanedAnswer && cleanedAnswer[0]) {
            logger.debug({ 
                requestId: requestIdentifier, 
                rawResponse: text, 
                cleanedAnswer: cleanedAnswer[0] 
            }, 'CAPTCHA solved');
            return cleanedAnswer[0];
        }
        
        logger.error({
            requestId: requestIdentifier,
            response: text.substring(0, 200)
        }, 'No clear numerical answer from Gemini');
        
        throw new Error('No clear numerical answer for CAPTCHA');
    } catch (error) {
        if (error.response && error.response.promptFeedback) {
            logger.error({
                requestId: requestIdentifier,
                promptFeedback: error.response.promptFeedback
            }, 'Gemini prompt feedback error');
        }
        
        throw new Error(`CAPTCHA solving failed: ${error.message}`);
    }
}

/**
 * Builds final URL for Ajax request
 */
function buildFinalUrl(diaryNo, year, scid, token, captchaAnswer) {
    const params = new URLSearchParams();
    params.append('diary_no', diaryNo);
    params.append('year', year);
    params.append('scid', scid);
    params.append(token.name, token.value);
    params.append('siwp_captcha_value', captchaAnswer);
    params.append('es_ajax_request', '1');
    params.append('submit', 'Search');
    params.append('action', 'get_case_status_diary_no');
    params.append('language', 'en');
    
    return `${AJAX_URL_BASE}?${params.toString()}`;
}

/**
 * Main process to get case details
 */
async function getCaseDetailsProcess(diaryNo, year, requestIdentifier = `${diaryNo}/${year}`) {
    let attempts = 0;
    let lastError = new Error("Process not attempted.");
    
    // Check cache first
    const cacheKey = `${diaryNo}/${year}`;
    const cachedData = responseCache.get(cacheKey);
    
    if (cachedData) {
        logger.info({ requestId: requestIdentifier }, 'Returning cached result');
        return cachedData;
    }

    while (attempts < MAX_ATTEMPTS) {
        attempts++;
        logger.info({ 
            requestId: requestIdentifier, 
            attempt: attempts, 
            maxAttempts: MAX_ATTEMPTS 
        }, 'Processing attempt started');
        
        try {
            // Add cache buster to initial URL
            const initialPageUrlWithCacheBuster = new URL(INITIAL_PAGE_URL);
            initialPageUrlWithCacheBuster.searchParams.set('_', Date.now().toString());

            // Fetch initial page
            const initialHtml = await fetchHtml(initialPageUrlWithCacheBuster.href);
            
            // Extract SCID
            const scid = extractSCID(initialHtml);
            logger.debug({ requestId: requestIdentifier, scid }, 'Extracted SCID');
            
            // Extract token
            const token = extractToken(initialHtml);
            logger.debug({ 
                requestId: requestIdentifier, 
                tokenName: token.name, 
                tokenValuePreview: token.value.substring(0, 10) + '...' 
            }, 'Extracted token');
            
            // Build captcha URL with cache buster
            const captchaImageUrl = new URL('?_siwp_captcha', CAPTCHA_IMAGE_BASE_URL);
            captchaImageUrl.searchParams.set('id', scid);
            captchaImageUrl.searchParams.set('ts', Date.now().toString());
            
            // Fetch and solve captcha
            const imageBuffer = await fetchImageBuffer(captchaImageUrl.href);
            const captchaAnswer = await solveCaptchaWithGemini(imageBuffer, requestIdentifier);
            logger.info({ 
                requestId: requestIdentifier, 
                captchaAnswer 
            }, 'CAPTCHA solved successfully');
            
            // Build final URL for Ajax request
            const finalUrl = buildFinalUrl(diaryNo, year, scid, token, captchaAnswer);
            
            // Set up controller for timeout
            const controller = new AbortController();
            const timeoutId = setTimeout(() => controller.abort(), REQUEST_TIMEOUT);
            
            try {
                // Make Ajax request
                const ajaxResponse = await fetch(finalUrl, {
                    headers: {
                        ...COMMON_HEADERS,
                        'Accept': 'application/json, text/javascript, */*; q=0.01',
                        'X-Requested-With': 'XMLHttpRequest',
                        'Referer': INITIAL_PAGE_URL,
                        'Cache-Control': 'no-cache',
                        'Pragma': 'no-cache',
                        'Expires': '0',
                    },
                    signal: controller.signal,
                    cache: 'no-store'
                });
                
                if (!ajaxResponse.ok) {
                    const errorText = await ajaxResponse.text();
                    throw new Error(`HTTP ${ajaxResponse.status}. Response: ${errorText.substring(0, 500)}`);
                }
                
                const responseData = await ajaxResponse.json();
                
                // Store successful result in cache
                responseCache.set(cacheKey, responseData);
                
                logger.info({ 
                    requestId: requestIdentifier,
                    success: responseData.success 
                }, 'Successfully fetched data');
                
                return responseData;
            } catch (error) {
                if (error.name === 'AbortError') {
                    throw new Error(`Ajax request timeout after ${REQUEST_TIMEOUT}ms`);
                }
                throw error;
            } finally {
                clearTimeout(timeoutId);
            }
        } catch (error) {
            lastError = error;
            logger.error({ 
                requestId: requestIdentifier, 
                attempt: attempts,
                error: error.message 
            }, 'Attempt failed');
            
            // Log stack trace only for unexpected errors
            if (!error.message.includes("SCID") && 
                !error.message.includes("token") && 
                !error.message.includes("CAPTCHA") && 
                !error.message.includes("HTTP") && 
                !error.message.includes("timeout")) {
                logger.error({ 
                    requestId: requestIdentifier,
                    stack: error.stack 
                }, 'Unexpected error details');
            }
        }
        
        if (attempts < MAX_ATTEMPTS) {
            // Exponential backoff with jitter
            const delaySeconds = Math.pow(2, attempts) + Math.random();
            logger.info({ 
                requestId: requestIdentifier,
                delaySeconds: delaySeconds.toFixed(2) 
            }, 'Waiting before retry');
            
            await new Promise(resolve => setTimeout(resolve, delaySeconds * 1000));
        }
    }
    
    logger.error({ 
        requestId: requestIdentifier,
        attempts: MAX_ATTEMPTS,
        finalError: lastError.message 
    }, 'All attempts failed');
    
    throw lastError;
}

// --- Express API Setup ---
function setupServer() {
    const app = express();
    const PORT = process.env.PORT || 3000;
    
    // Add middlewares for performance and security
    app.use(compression()); // Compress responses
    app.use(helmet()); // Security headers
    app.use(express.json({ limit: '1mb' })); // Increase payload limit if needed
    
    // Add request logging middleware
    app.use((req, res, next) => {
        const start = process.hrtime();
        
        res.on('finish', () => {
            const [seconds, nanoseconds] = process.hrtime(start);
            const duration = seconds * 1000 + nanoseconds / 1000000;
            
            logger.info({
                method: req.method,
                url: req.originalUrl,
                status: res.statusCode,
                duration: `${duration.toFixed(2)}ms`,
                ip: req.ip,
                userAgent: req.get('user-agent')
            }, 'Request completed');
        });
        
        next();
    });
    
    // Add health check endpoint
    app.get('/health', (req, res) => {
        res.status(200).json({
            status: 'UP',
            timestamp: new Date().toISOString(),
            uptime: process.uptime(),
            environment: process.env.NODE_ENV || 'development',
            version: process.env.npm_package_version || 'unknown'
        });
    });
    
    // Main API endpoint
    app.post('/api/case-details', async (req, res) => {
        const startTime = process.hrtime();
        const { diaryData } = req.body;
        const requestIdentifier = `req_${Date.now()}_${diaryData || 'unknown'}`;
        
        logger.info({ 
            requestId: requestIdentifier,
            diaryData 
        }, 'API call received');
        
        // Validate input
        if (!diaryData || typeof diaryData !== 'string') {
            const timeTakenMs = calculateTimeTaken(startTime);
            logger.warn({ 
                requestId: requestIdentifier,
                timeTakenMs 
            }, 'Invalid payload');
            
            return res.status(400).json({
                success: false,
                error: "Invalid payload: 'diaryData' (string 'number/year') is required.",
                timeTakenMs
            });
        }
        
        const parts = diaryData.split('/');
        if (parts.length !== 2 || !/^\d+$/.test(parts[0]) || !/^\d{4}$/.test(parts[1])) {
            const timeTakenMs = calculateTimeTaken(startTime);
            logger.warn({ 
                requestId: requestIdentifier,
                timeTakenMs,
                diaryData 
            }, 'Invalid diaryData format');
            
            return res.status(400).json({
                success: false,
                error: "Invalid 'diaryData' format: Expected 'number/year' (e.g., '2444/2023').",
                timeTakenMs
            });
        }
        
        const diaryNo = parts[0];
        const year = parts[1];
        
        try {
            const ajaxJsonResponse = await getCaseDetailsProcess(diaryNo, year, requestIdentifier);
            const timeTakenMs = calculateTimeTaken(startTime);
            
            logger.info({ 
                requestId: requestIdentifier,
                timeTakenMs 
            }, 'Request successful');
            
            res.status(200).json({
                success: true,
                data: ajaxJsonResponse,
                timeTakenMs,
                cached: responseCache.has(`${diaryNo}/${year}`)
            });
        } catch (error) {
            const timeTakenMs = calculateTimeTaken(startTime);
            
            logger.error({ 
                requestId: requestIdentifier,
                error: error.message,
                timeTakenMs 
            }, 'Request failed');
            
            res.status(500).json({
                success: false,
                error: `Failed to retrieve case details: ${error.message}`,
                timeTakenMs
            });
        }
    });
    
    return app;
}

function calculateTimeTaken(startTime) {
    const endTime = process.hrtime(startTime);
    return (endTime[0] * 1000 + endTime[1] / 1000000).toFixed(2);
}

// --- Server Startup Logic ---
if (require.main === module) {
    // Use clustering for better performance if enabled
    if (ENABLE_CLUSTERING && cluster.isPrimary) {
        const numCPUs = os.cpus().length;
        logger.info({ cpuCount: numCPUs }, 'Starting server with clustering');
        
        // Fork workers for each CPU
        for (let i = 0; i < numCPUs; i++) {
            cluster.fork();
        }
        
        cluster.on('exit', (worker, code, signal) => {
            logger.warn({ 
                workerId: worker.id,
                code,
                signal 
            }, 'Worker died, spawning replacement');
            
            cluster.fork(); // Replace dead workers
        });
    } else {
        const app = setupServer();
        const PORT = process.env.PORT || 3000;
        
        const serverInstance = app.listen(PORT, () => {
            logger.info({ 
                port: PORT,
                env: process.env.NODE_ENV || 'development',
                geminiModel: GEMINI_MODEL_NAME,
                pid: process.pid,
                clustering: ENABLE_CLUSTERING ? 'enabled' : 'disabled',
                cacheTTL: `${CACHE_TTL} seconds`,
                maxAttempts: MAX_ATTEMPTS,
                requestTimeout: `${REQUEST_TIMEOUT}ms`
            }, 'Server started');
        });
        
        // Configure server timeout
        serverInstance.timeout = REQUEST_TIMEOUT + 5000; // A bit longer than request timeout
        
        function gracefulShutdown(signal) {
            logger.info({ signal }, 'Shutdown signal received');
            
            serverInstance.close(() => {
                logger.info('HTTP server closed');
                
                // Close any other resources
                responseCache.close();
                
                process.exit(0);
            });
            
            // Force exit if graceful shutdown fails
            setTimeout(() => {
                logger.error('Forced shutdown after timeout');
                process.exit(1);
            }, 10000);
        }
        
        process.on('SIGTERM', () => gracefulShutdown('SIGTERM'));
        process.on('SIGINT', () => gracefulShutdown('SIGINT'));
        process.on('uncaughtException', (error) => {
            logger.fatal({ 
                error: error.message,
                stack: error.stack 
            }, 'Uncaught exception');
            
            gracefulShutdown('UNCAUGHT_EXCEPTION');
        });
    }
}

module.exports = { 
    setupServer,
    getCaseDetailsProcess 
}; // Export for testing