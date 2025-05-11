// --- Dependencies ---
const express = require('express');
const cheerio = require('cheerio');
const { GoogleGenerativeAI, HarmCategory, HarmBlockThreshold } = require('@google/generative-ai');
const { URL, URLSearchParams } = require('url');
const {config} = require('dotenv');

config(); // Load environment variables from .env file

// --- Configuration ---
const INITIAL_PAGE_URL = 'https://www.sci.gov.in/case-status-diary-no/';
const AJAX_URL_BASE = 'https://www.sci.gov.in/wp-admin/admin-ajax.php';

const GEMINI_API_KEY = process.env.GEMINI_API_KEY;
const GEMINI_MODEL_NAME = "gemini-2.0-flash";

if (!GEMINI_API_KEY) {
    console.error("CRITICAL_ERROR: GOOGLE_API_KEY environment variable is not set. API cannot function.");
    process.exit(1); // Essential for API to work
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
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
    'Accept-Language': 'en-US,en;q=0.9',
};

// --- Core Logic Functions (Minimal Logging) ---

async function fetchHtml(url, options = {}) {
    const response = await fetch(url, { headers: { ...COMMON_HEADERS, ...options.headers } });
    if (!response.ok) {
        throw new Error(`FetchHTML Error: HTTP ${response.status} for ${url}`);
    }
    return await response.text();
}

async function fetchImageBuffer(url) {
    const response = await fetch(url, { headers: COMMON_HEADERS });
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
    throw new Error('CSRF token not found.');
}

function extractCaptchaInfo(html) {
    const $ = cheerio.load(html);
    const captchaImageElement = $('img#siwp_captcha_image_0');
    if (captchaImageElement.length > 0) {
        const imgSrc = captchaImageElement.attr('src');
        if (imgSrc) {
            const absoluteImgSrc = new URL(imgSrc, INITIAL_PAGE_URL).href;
            const urlParams = new URL(absoluteImgSrc).searchParams;
            const imgId = urlParams.get('id');
            if (imgId) return { imageUrl: absoluteImgSrc, imgId: imgId };
            throw new Error('CAPTCHA image ID ("id" param) not found in src.');
        }
    }
    throw new Error('CAPTCHA image element (img#siwp_captcha_image_0) not found.');
}

async function solveCaptchaWithGemini(imageBuffer) {
    try {
        const result = await model.generateContent([
            "Solve this numerical CAPTCHA. It is a simple arithmetic expression (e.g., '7 - 2'). Provide ONLY the numerical result of the expression (e.g., '5'). Do not include the original expression or any other text.",
            { inlineData: { data: imageBuffer.toString('base64'), mimeType: 'image/png' } },
        ]);
        const response = result.response;
        const text = response.text();
        const cleanedAnswer = text.trim().match(/-?\d+/);
        if (cleanedAnswer && cleanedAnswer[0]) return cleanedAnswer[0];
        
        console.error("Gemini Error: No clear numerical answer. Response:", text.substring(0, 200)); // Log Gemini's actual response if unclear
        throw new Error('Gemini Error: Failed to get clear numerical answer for CAPTCHA.');
    } catch (error) {
        // Log the original error message if it's not the one we threw
        if (!error.message.startsWith('Gemini Error:')) {
            console.error("Gemini API Call Error:", error.message || error);
        }
        if (error.response && error.response.promptFeedback) {
            console.error("Gemini Prompt Feedback:", error.response.promptFeedback);
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

function parseCaseDetailsFromHtml(htmlString) {
    const $ = cheerio.load(htmlString);
    const caseRow = $('div.distTableContent table tbody tr').first();
    if (caseRow.length > 0) {
        const caseData = {
            serialNumber: caseRow.find('td:nth-child(1)').text().trim(),
            diaryNumber: caseRow.find('td:nth-child(2)').text().trim(),
            caseNumber: caseRow.find('td:nth-child(3)').text().trim(),
            petitionerName: (caseRow.find('td.petitioners').text().trim() || caseRow.find('td:nth-child(4)').text().trim()).replace(/\s\s+/g, ' '),
            respondentName: (caseRow.find('td.respondents').text().trim() || caseRow.find('td:nth-child(5)').text().trim()).replace(/\s\s+/g, ' '),
            status: caseRow.find('td:nth-child(6)').text().trim(),
            actionViewLink: null,
        };
        const viewLink = caseRow.find('td:nth-child(7) a.viewCnrDetails').attr('href');
        if (viewLink) caseData.actionViewLink = new URL(viewLink, INITIAL_PAGE_URL).href;
        
        if (caseData.diaryNumber) return caseData; // Basic validation
    }
    throw new Error("CaseDetailsParse Error: Could not parse details from resultsHTML.");
}

async function getCaseDetailsProcess(diaryNo, year) {
    const MAX_ATTEMPTS = 5;
    let attempts = 0;
    let lastError = new Error("Process not attempted."); // Default error

    while (attempts < MAX_ATTEMPTS) {
        attempts++;
        try {
            const initialHtml = await fetchHtml(INITIAL_PAGE_URL);
            const token = extractToken(initialHtml);
            const captchaInfo = extractCaptchaInfo(initialHtml);
            const scid = captchaInfo.imgId;
            const imageBuffer = await fetchImageBuffer(captchaInfo.imageUrl);
            const captchaAnswer = await solveCaptchaWithGemini(imageBuffer);
            const finalUrl = buildFinalUrl(diaryNo, year, scid, token, captchaAnswer);

            const ajaxResponse = await fetch(finalUrl, {
                headers: { ...COMMON_HEADERS, 'X-Requested-With': 'XMLHttpRequest', 'Accept': 'application/json, text/javascript, */*; q=0.01' }
            });

            if (!ajaxResponse.ok) {
                const errorText = await ajaxResponse.text();
                throw new Error(`AJAX Error: HTTP ${ajaxResponse.status}. Response: ${errorText.substring(0, 200)}`);
            }
            const jsonData = await ajaxResponse.json();

            if (jsonData && jsonData.success && jsonData.data && jsonData.data.resultsHtml) {
                return parseCaseDetailsFromHtml(jsonData.data.resultsHtml); // Success
            } else {
                throw new Error(`AJAX Error: Response unsuccessful or malformed. Data: ${JSON.stringify(jsonData).substring(0,200)}`);
            }
        } catch (error) {
            lastError = error; // Store the most recent error
            console.error(`Attempt ${attempts}/${MAX_ATTEMPTS} for ${diaryNo}/${year} failed: ${error.message}`);
            // Specific conditions that warrant immediate failure or different handling could be added here.
            // For now, all errors lead to a retry if attempts remain.
        }
        if (attempts < MAX_ATTEMPTS) {
            const delaySeconds = 2 + attempts; // Small, increasing delay
            await new Promise(resolve => setTimeout(resolve, delaySeconds * 1000));
        }
    }
    // If loop finishes, all attempts failed. Throw the last encountered error.
    console.error(`All ${MAX_ATTEMPTS} attempts failed for ${diaryNo}/${year}. Final error: ${lastError.message}`);
    throw lastError;
}

// --- Express API Setup ---
const app = express();
const PORT = process.env.PORT;

app.use(express.json()); // Middleware to parse JSON bodies

app.post('/api/case-details', async (req, res) => {
    const startTime = process.hrtime();

    const { diaryData } = req.body;

    if (!diaryData || typeof diaryData !== 'string') {
        const timeTakenMs = parseFloat(calculateTimeTaken(startTime));
        return res.status(400).json({
            success: false,
            error: "Invalid payload: 'diaryData' (string 'number/year') is required.",
            timeTakenMs
        });
    }

    const parts = diaryData.split('/');
    if (parts.length !== 2 || !/^\d+$/.test(parts[0]) || !/^\d{4}$/.test(parts[1])) {
        const timeTakenMs = parseFloat(calculateTimeTaken(startTime));
        return res.status(400).json({
            success: false,
            error: "Invalid 'diaryData' format: Expected 'number/year' (e.g., '2444/2023').",
            timeTakenMs
        });
    }

    const diaryNo = parts[0];
    const year = parts[1];

    try {
        const caseDetails = await getCaseDetailsProcess(diaryNo, year);
        const timeTakenMs = parseFloat(calculateTimeTaken(startTime));
        res.status(200).json({
            success: true,
            data: caseDetails,
            timeTakenMs
        });
    } catch (error) {
        const timeTakenMs = parseFloat(calculateTimeTaken(startTime));
        // The getCaseDetailsProcess logs its own attempt failures.
        // The error thrown by it will be its lastError.
        res.status(500).json({
            success: false,
            error: `Failed to retrieve case details: ${error.message}`,
            timeTakenMs
        });
    }
});

function calculateTimeTaken(startTime) {
    const endTime = process.hrtime(startTime);
    // Convert to milliseconds and fix to 2 decimal places
    return (endTime[0] * 1000 + endTime[1] / 1000000).toFixed(2);
}

app.listen(PORT, '0.0.0.0', () => {
    console.log(`Case Details API Server running on http://0.0.0.0:${PORT}`);
});

