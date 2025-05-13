// --- Dependencies ---
const express = require('express');
const cheerio = require('cheerio');
const { GoogleGenerativeAI, HarmCategory, HarmBlockThreshold } = require('@google/generative-ai');
const { URL, URLSearchParams } = require('url'); // URL is already here
const { config } = require('dotenv');

config(); // Load environment variables from .env file

// --- Configuration ---
const INITIAL_PAGE_URL = 'https://www.sci.gov.in/case-status-diary-no/';
const AJAX_URL_BASE = 'https://www.sci.gov.in/wp-admin/admin-ajax.php';
const CAPTCHA_IMAGE_BASE_URL = 'https://www.sci.gov.in/'; // Used for resolving relative links

const GEMINI_API_KEY = process.env.GEMINI_API_KEY;
const GEMINI_MODEL_NAME = "gemini-2.0-flash"; // Corrected from gemini-1.5-flash-latest to a valid model if needed

if (!GEMINI_API_KEY) {
    console.error("CRITICAL_ERROR: GEMINI_API_KEY environment variable is not set. API cannot function.");
    process.exit(1);
}

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
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.4844.51 Safari/537.36',
    'Accept-Language': 'en-US,en;q=0.9',
};

// --- Core Logic Functions ---

async function fetchHtml(url, options = {}) {
    console.log(`[fetchHtml] Fetching URL: ${url}`);
    const fetchOptions = {
        headers: {
            ...COMMON_HEADERS,
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
            'Cache-Control': 'no-cache, no-store, must-revalidate',
            'Pragma': 'no-cache', // HTTP/1.0
            'Expires': '0', // Proxies
            ...options.headers
        },
        cache: 'no-store' // Node.js fetch specific
    };
    const response = await fetch(url, fetchOptions);
    if (!response.ok) {
        throw new Error(`FetchHTML Error: HTTP ${response.status} for ${url}`);
    }
    return await response.text();
}

async function fetchImageBuffer(url) {
    console.log(`[fetchImageBuffer] Fetching image URL: ${url}`);
    const fetchOptions = {
        headers: {
            ...COMMON_HEADERS,
            'Accept': 'image/avif,image/webp,image/apng,image/svg+xml,image/*,*/*;q=0.8',
            'Cache-Control': 'no-cache, no-store, must-revalidate',
            'Pragma': 'no-cache',
            'Expires': '0',
        },
        cache: 'no-store'
    };
    const response = await fetch(url, fetchOptions);
    if (!response.ok) {
        throw new Error(`FetchImageBuffer Error: HTTP ${response.status} for image ${url}`);
    }
    const arrayBuffer = await response.arrayBuffer();
    return Buffer.from(arrayBuffer);
}

function extractToken(html) {
    const $ = cheerio.load(html);
    const tokenInput = $('input[type="hidden"][name^="tok_"]');
    if (tokenInput.length > 0) {
        const tokenName = tokenInput.first().attr('name');
        const tokenValue = tokenInput.first().attr('value');
        if (tokenName && tokenValue) return { name: tokenName, value: tokenValue };
    }
    throw new Error('CSRF token (input[name^="tok_"]) not found.');
}

async function solveCaptchaWithGemini(imageBuffer, requestIdentifier) {
    try {
        console.log(`[${requestIdentifier}] Calling Gemini to solve CAPTCHA.`);
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
            console.log(`[${requestIdentifier}] Gemini CAPTCHA raw response: "${text}", Cleaned answer: "${cleanedAnswer[0]}"`);
            return cleanedAnswer[0];
        }
        
        console.error(`[${requestIdentifier}] Gemini Error: No clear numerical answer. Response:`, text.substring(0, 200));
        throw new Error('Gemini Error: Failed to get clear numerical answer for CAPTCHA.');
    } catch (error) {
        if (!error.message.startsWith('Gemini Error:')) {
            console.error(`[${requestIdentifier}] Gemini API Call Error:`, error.message || error);
        }
        if (error.response && error.response.promptFeedback) {
            console.error(`[${requestIdentifier}] Gemini Prompt Feedback:`, error.response.promptFeedback);
        }
        throw new Error(`Gemini CAPTCHA solving failed: ${error.message}`);
    }
}

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

async function getCaseDetailsProcess(diaryNo, year, requestIdentifier = `${diaryNo}/${year}`) {
    const MAX_ATTEMPTS = 3;
    let attempts = 0;
    let lastError = new Error("Process not attempted.");

    while (attempts < MAX_ATTEMPTS) {
        attempts++;
        console.log(`[${requestIdentifier}] Attempt ${attempts}/${MAX_ATTEMPTS} starting.`);
        try {
            const initialPageUrlWithCacheBuster = new URL(INITIAL_PAGE_URL);
            initialPageUrlWithCacheBuster.searchParams.set('_', Date.now().toString()); // Cache buster for initial page

            const initialHtml = await fetchHtml(initialPageUrlWithCacheBuster.href);
            const $initialPage = cheerio.load(initialHtml);

            const scidInput = $initialPage('input#input_siwp_captcha_id_0[name="scid"]');
            if (scidInput.length === 0) {
                throw new Error('SCID input field (input#input_siwp_captcha_id_0[name="scid"]) not found.');
            }
            const scid = scidInput.val();
            if (!scid) {
                throw new Error('SCID value is empty.');
            }
            console.log(`[${requestIdentifier}] Extracted SCID: ${scid}`);

            const token = extractToken(initialHtml);
            console.log(`[${requestIdentifier}] Extracted Token Name: ${token.name}, Token Value: ${token.value.substring(0,10)}...`);

            const captchaImageUrl = new URL('?_siwp_captcha', CAPTCHA_IMAGE_BASE_URL);
            captchaImageUrl.searchParams.set('id', scid);
            captchaImageUrl.searchParams.set('ts', Date.now().toString()); // Timestamp for cache busting image
            console.log(`[${requestIdentifier}] CAPTCHA Image URL: ${captchaImageUrl.href}`);

            const imageBuffer = await fetchImageBuffer(captchaImageUrl.href);
            const captchaAnswer = await solveCaptchaWithGemini(imageBuffer, requestIdentifier);
            console.log(`[${requestIdentifier}] Gemini CAPTCHA Solved Answer: ${captchaAnswer}`);

            const finalUrl = buildFinalUrl(diaryNo, year, scid, token, captchaAnswer);
            console.log(`[${requestIdentifier}] Constructed Final AJAX URL: ${finalUrl}`);

            const ajaxResponse = await fetch(finalUrl, {
                headers: {
                    ...COMMON_HEADERS,
                    'Accept': 'application/json, text/javascript, */*; q=0.01',
                    'X-Requested-With': 'XMLHttpRequest',
                    'Referer': INITIAL_PAGE_URL,
                    'Cache-Control': 'no-cache, no-store, must-revalidate',
                    'Pragma': 'no-cache',
                    'Expires': '0',
                },
                cache: 'no-store'
            });

            if (!ajaxResponse.ok) {
                const errorText = await ajaxResponse.text();
                throw new Error(`AJAX Error: HTTP ${ajaxResponse.status} for ${finalUrl}. Response: ${errorText.substring(0, 500)}`);
            }

            const responseData = await ajaxResponse.json();
            console.log(`[${requestIdentifier}] Successfully fetched data. AJAX response status: ${responseData.success}`);
            return responseData;

        } catch (error) {
            lastError = error;
            console.error(`[${requestIdentifier}] Attempt ${attempts}/${MAX_ATTEMPTS} failed: ${error.message}`);
            if (!error.message.includes("SCID") && !error.message.includes("token") && !error.message.includes("CAPTCHA") && !error.message.includes("AJAX Error") && !error.message.includes("Fetch")) {
                console.error(error.stack);
            }
        }
        if (attempts < MAX_ATTEMPTS) {
            const delaySeconds = Math.pow(2, attempts) + Math.random(); // Exponential backoff with jitter
            console.log(`[${requestIdentifier}] Waiting ${delaySeconds.toFixed(2)} seconds before retry...`);
            await new Promise(resolve => setTimeout(resolve, delaySeconds * 1000));
        }
    }
    console.error(`[${requestIdentifier}] All ${MAX_ATTEMPTS} attempts failed. Final error: ${lastError.message}`);
    throw lastError;
}


// --- Helper function to parse HTML and extract case details ---
function parseCaseDetailsHtml(htmlString) {
    const $ = cheerio.load(htmlString);
    const cases = [];
    // The relevant table is inside div.distTableContent
    const caseRows = $('div.distTableContent table tbody tr');

    // Check if the relevant content area indicates no records.
    // This check is broad; refine if the site has a very specific "no records" element.
    if (caseRows.length === 0) {
        const contentDivText = $('div.distTableContent').text().toLowerCase();
        if (contentDivText.includes('no record found') || contentDivText.includes('no data found')) {
            console.log("[parseCaseDetailsHtml] No records found message detected.");
            return []; // Return empty array for no records
        }
        // If no rows and no explicit "no records" message, still return empty.
        console.log("[parseCaseDetailsHtml] No case rows found in the table.");
        return [];
    }

    caseRows.each((index, element) => {
        const row = $(element);
        const cells = row.find('td');

        if (cells.length >= 7) { // Ensure we have enough cells
            const caseDetail = {
                serialNumber: $(cells[0]).text().trim(),
                diaryNumber: $(cells[1]).text().trim(),
                caseNumber: '',
                registeredOn: '',
                petitionerName: $(cells[3]).text().trim(),
                respondentName: $(cells[4]).text().trim(),
                status: $(cells[5]).text().trim(),
                viewDetailsLink: '',
            };

            // Extract and split Case Number and Registration Date from the 3rd cell (index 2)
            const caseInfoFullText = $(cells[2]).text().trim();
            const registeredOnMatch = caseInfoFullText.match(/Registered on\s*([\d]{2}-[\d]{2}-[\d]{4})/i);

            if (registeredOnMatch && registeredOnMatch[1]) {
                caseDetail.registeredOn = registeredOnMatch[1];
                caseDetail.caseNumber = caseInfoFullText.replace(registeredOnMatch[0], '').replace(/\s+/g, ' ').trim();
            } else {
                caseDetail.caseNumber = caseInfoFullText.replace(/\s+/g, ' ').trim(); // Fallback if no date found
            }

            // Extract View Details Link from the 7th cell (index 6)
            const viewLinkTag = $(cells[6]).find('a.viewCnrDetails'); // Class specific to the view link
            if (viewLinkTag.length > 0) {
                const relativeLink = viewLinkTag.attr('href');
                if (relativeLink) {
                    try {
                        // CAPTCHA_IMAGE_BASE_URL is 'https://www.sci.gov.in/'
                        caseDetail.viewDetailsLink = new URL(relativeLink, CAPTCHA_IMAGE_BASE_URL).href;
                    } catch (e) {
                        console.error(`[parseCaseDetailsHtml] Error constructing URL for viewDetailsLink ('${relativeLink}'):`, e.message);
                        caseDetail.viewDetailsLink = relativeLink; // Fallback to relative link on error
                    }
                }
            }
            cases.push(caseDetail);
        } else {
            console.warn("[parseCaseDetailsHtml] Found a row with fewer than 7 cells, skipping.");
        }
    });

    return cases;
}


// --- Express API Setup ---
const app = express();
const PORT = process.env.PORT || 3000;

app.use(express.json());

app.post('/api/case-details', async (req, res) => {
    const startTime = process.hrtime();
    const { diaryData } = req.body;
    const requestIdentifierLog = diaryData || `unknown_diary_data_${Date.now()}`;

    console.log(`\n--- [API Call Start - ${requestIdentifierLog}] Received request for diaryData: ${diaryData} ---`);

    if (!diaryData || typeof diaryData !== 'string') {
        const timeTakenMs = parseFloat(calculateTimeTaken(startTime));
        console.log(`[${requestIdentifierLog}] Invalid payload. Responding 400.`);
        return res.status(400).json({
            success: false,
            error: "Invalid payload: 'diaryData' (string 'number/year') is required.",
            timeTakenMs
        });
    }

    const parts = diaryData.split('/');
    if (parts.length !== 2 || !/^\d+$/.test(parts[0]) || !/^\d{4}$/.test(parts[1])) {
        const timeTakenMs = parseFloat(calculateTimeTaken(startTime));
         console.log(`[${requestIdentifierLog}] Invalid diaryData format. Responding 400.`);
        return res.status(400).json({
            success: false,
            error: "Invalid 'diaryData' format: Expected 'number/year' (e.g., '2444/2023').",
            timeTakenMs
        });
    }

    const diaryNo = parts[0];
    const year = parts[1];

    try {
        // sourceResponse is the JSON object returned by the target website's AJAX endpoint
        const sourceResponse = await getCaseDetailsProcess(diaryNo, year, requestIdentifierLog);
        const timeTakenMs = parseFloat(calculateTimeTaken(startTime));

        if (sourceResponse && sourceResponse.success === true && sourceResponse.data && sourceResponse.data.resultsHtml) {
            const structuredCaseData = parseCaseDetailsHtml(sourceResponse.data.resultsHtml);
            
            console.log(`[${requestIdentifierLog}] Request successful. Parsed ${structuredCaseData.length} case(s). Responding 200.`);
            res.status(200).json({
                success: true,
                data: structuredCaseData, // The array of parsed case objects
                pagination: sourceResponse.data.pagination, // Pass along pagination info
                timeTakenMs
            });
        } else if (sourceResponse && sourceResponse.success === false) {
            // The target website's AJAX call reported an error (e.g., wrong captcha, no data)
            const errorMessage = (sourceResponse.data && (sourceResponse.data.message || sourceResponse.data.html)) 
                                 || "Failed to retrieve details: Source reported an error.";
            console.warn(`[${requestIdentifierLog}] Source AJAX call reported failure: ${errorMessage}. Responding 422.`);
            res.status(422).json({ // 422 Unprocessable Entity: server understands request, but cannot process due to source issue
                success: false,
                error: errorMessage,
                // originalSourceData: sourceResponse.data, // Optionally include original error data from source for debugging
                timeTakenMs
            });
        } else {
            // Unexpected structure from the sourceResponse
            console.error(`[${requestIdentifierLog}] Unexpected response structure from source. Responding 500.`);
            res.status(500).json({
                success: false,
                error: "Failed to process response from source: Unexpected data structure.",
                // originalSourceResponse: sourceResponse, // For debugging
                timeTakenMs
            });
        }
    } catch (error) { // This catches errors from getCaseDetailsProcess itself (network, Gemini, local logic)
        const timeTakenMs = parseFloat(calculateTimeTaken(startTime));
        console.error(`[${requestIdentifierLog}] Request processing failed. Error: ${error.message}. Time taken: ${timeTakenMs}ms`);
        res.status(500).json({
            success: false,
            error: `Failed to retrieve case details: ${error.message}`,
            timeTakenMs
        });
    }
    console.log(`--- [API Call End - ${requestIdentifierLog}] ---`);
});

function calculateTimeTaken(startTime) {
    const endTime = process.hrtime(startTime);
    return (endTime[0] * 1000 + endTime[1] / 1000000).toFixed(2);
}

let serverInstance;
if (require.main === module) {
    serverInstance = app.listen(PORT, () => {
        console.log(`Case Details API Server running on http://localhost:${PORT}`);
        if (!GEMINI_API_KEY) {
            console.warn("CRITICAL_WARNING: GEMINI_API_KEY is NOT SET. The API will not function as expected.");
        }
        console.log(`Using Gemini Model: ${GEMINI_MODEL_NAME}`);
    });
}

function gracefulShutdown(signal) {
    console.log(`${signal} signal received: closing HTTP server`);
    if (serverInstance) {
        serverInstance.close(() => {
            console.log('HTTP server closed.');
            process.exit(0);
        });
    } else {
        process.exit(0);
    }
}

process.on('SIGTERM', () => gracefulShutdown('SIGTERM'));
process.on('SIGINT', () => gracefulShutdown('SIGINT'));

module.exports = app;