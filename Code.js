// ==========================================
// CONFIGURATION (TEST MODE)
// ==========================================
const CONFIG = {
  email: "firebase-adminsdk-fbsvc@nicucounselingsheet.iam.gserviceaccount.com",
  // Store your private key in Project Settings > Script Properties with key 'FIREBASE_KEY'
  key: PropertiesService.getScriptProperties().getProperty('FIREBASE_KEY').replace(/\\n/g, '\n'),
  projectId: "nicucounselingsheet",
  // Store your API key in Project Settings > Script Properties with key 'GEMINI_API_KEY'
  geminiApiKey: PropertiesService.getScriptProperties().getProperty('GEMINI_API_KEY'),
  emailQuery: 'has:attachment -label:Charted',
  pathPatients: 'artifacts/nicu-dashboard-hybrid-test/public/data/patients',
  pathCharts: 'artifacts/nicu-dashboard-hybrid-test/public/data/medical_charts',
  pathInbox: 'artifacts/nicu-dashboard-hybrid-test/public/data/lab_inbox',
  pathBrain: 'artifacts/nicu-dashboard-hybrid-test/config/gemini_brain',
  pathNotifications: 'artifacts/nicu-dashboard-hybrid-test/public/data/notifications'
};

const firestore = FirestoreApp.getFirestore(CONFIG.email, CONFIG.key, CONFIG.projectId);

// ==========================================
// MAIN TRIGGER FUNCTION
// ==========================================
function setupTrigger() {
  // 1. Run this function ONCE manually to start the automation.
  // 2. It creates a "Time-Driven" trigger that runs processLabReports every 5 minutes.
  
  const triggers = ScriptApp.getProjectTriggers();
  const triggerName = 'processLabReports';
  
  // Prevent creating duplicates
  if (triggers.some(t => t.getHandlerFunction() === triggerName)) {
    console.log("Trigger already exists.");
    return;
  }

  ScriptApp.newTrigger(triggerName).timeBased().everyMinutes(5).create();
  console.log("Automation started! Checking emails every 5 minutes.");
}

function processLabReports() {
  // 0. LOCK SERVICE (Prevent Overlap)
  const lock = LockService.getScriptLock();
  if (!lock.tryLock(30000)) {
    console.log("⚠️ Process is already running. Skipping.");
    return;
  }

  try {
  // Ensure the label exists to mark processed emails without deleting them
  const labelName = "Charted";
  let label = GmailApp.getUserLabelByName(labelName);
  if (!label) label = GmailApp.createLabel(labelName);

  // Cleanup old label if exists to prevent dual labeling
  const oldLabel = GmailApp.getUserLabelByName("NICU_PROCESSED");

  // BATCHING CONFIGURATION
  const EMAIL_BATCH_SIZE = 15; // Process max 15 threads per execution
  const GEMINI_BATCH_SIZE = 10; // Send 10 PDFs per API request (User Request)
  const MAX_EXECUTION_TIME = 240000; // 4 minutes (Safe buffer within Google's 6-min limit)
  const startTime = Date.now();

  // Fetch only a batch of threads (0 to BATCH_SIZE)
  const threads = GmailApp.search(CONFIG.emailQuery, 0, EMAIL_BATCH_SIZE);
  
  if (threads.length === 0) {
    console.log("No test emails found.");
    return;
  }

  // 1. Fetch "Gemini Brain" Instructions (User Feedback)
  let userInstructions = "";
  try {
    const doc = firestore.getDocument(CONFIG.pathBrain);
    if (doc && doc.fields && doc.fields.instructions) {
      userInstructions = doc.fields.instructions.stringValue;
      console.log("🧠 Loaded User Instructions for Gemini.");
    }
  } catch (e) { console.log("No custom AI instructions found yet."); }

  // 1. Fetch Patients
  const allPatients = firestore.getDocuments(CONFIG.pathPatients).map(doc => {
    const data = doc.fields;
    return {
      id: doc.name.split('/').pop(),
      name: data.name ? data.name.stringValue : "",
      ward: data.ward ? data.ward.stringValue : "",
      serial: data.customSerial ? data.customSerial.integerValue : ""
    };
  });

  console.log(`Scanning ${threads.length} threads for PDFs...`);

  let pdfQueue = [];
  const processedFiles = new Set(); // Track unique files to prevent duplicates

  for (const thread of threads) {
    // SAFETY CHECK: Stop if we are running out of time
    if (Date.now() - startTime > MAX_EXECUTION_TIME) {
      console.log("⏳ Time limit approaching. Stopping batch to prevent timeout. Will resume in next run.");
      break;
    }

    const messages = thread.getMessages();
    
    // 1. Check Subject Line for Patient Match (Priority 1)
    const subject = thread.getFirstMessageSubject(); // Use thread subject
    let messageLevelMatch = null;
    const subjectResult = findBestMatch(subject, allPatients);
    if (subjectResult.score > 75) {
      messageLevelMatch = subjectResult;
      console.log(`[MATCH] Found patient in Subject: "${subjectResult.patient.name}" (Score: ${subjectResult.score})`);
    }

    // Iterate through ALL messages in the thread to find attachments
    for (const msg of messages) {
      const attachments = msg.getAttachments();
      
      // Filter for PDFs and select the smallest one (avoid graphic-heavy duplicates)
      const pdfs = attachments.filter(a => {
        const type = a.getContentType().toLowerCase();
        const name = a.getName().toLowerCase();
        return type === "application/pdf" || name.endsWith(".pdf");
      });
      
      if (pdfs.length > 0) {
          // 1. SELECT SMALLEST PDF (Handle Letterhead vs Non-Letterhead duplicates)
          pdfs.sort((a, b) => a.getSize() - b.getSize());
          const att = pdfs[0]; // Use the smallest file
          
          if (pdfs.length > 1) {
            console.log(`[SMART SELECT] Selected smallest PDF: "${att.getName()}" (${att.getSize()} bytes). Ignored ${pdfs.length - 1} larger variants (e.g. Letterhead).`);
          }
          
          // DEDUPLICATION: Check if we've already queued this exact file (e.g. from reply chains)
          const fileSignature = `${att.getName()}_${att.getSize()}`;
          if (processedFiles.has(fileSignature)) {
            console.log(`[DUPLICATE] Skipping "${att.getName()}" (already queued).`);
            continue;
          }
          processedFiles.add(fileSignature);
            
          // 2. Check Filename for Patient Match (Priority 2)
          let currentMatch = messageLevelMatch;
          if (!currentMatch) {
            const fileResult = findBestMatch(att.getName(), allPatients);
            if (fileResult.score > 75) {
              currentMatch = fileResult;
            }
          }

          // Add to Queue
          pdfQueue.push({
            blob: att,
            filename: att.getName(),
            preMatch: currentMatch,
            thread: thread
          });
      }
    }
  }

  console.log(`Queued ${pdfQueue.length} PDFs. Processing in batches of ${GEMINI_BATCH_SIZE}...`);

  // --- PROCESS BATCHES ---
  for (let i = 0; i < pdfQueue.length; i += GEMINI_BATCH_SIZE) {
    const batch = pdfQueue.slice(i, i + GEMINI_BATCH_SIZE);
    const batchThreads = new Set(); // Track threads for THIS batch only
    console.log(`🚀 Sending Batch ${Math.floor(i/GEMINI_BATCH_SIZE) + 1} (${batch.length} files) to Gemini...`);

    try {
      // CALL GEMINI WITH MULTIPLE FILES
      const results = analyzeBatchWithGemini(batch, userInstructions);

      if (!results) {
        console.error(`Batch failed. Skipping batch.`);
        continue;
      }

      // PROCESS EACH RESULT
      for (let j = 0; j < batch.length; j++) {
        const item = batch[j];
        
        // Match result to file by filename (User confirmed filenames are unique)
        let labData = results.find(r => r.filename === item.filename);
        if (!labData) {
          // Fallback: Try case-insensitive match
          labData = results.find(r => r.filename && r.filename.toLowerCase() === item.filename.toLowerCase());
        }

        if (labData) {
              let currentMatch = item.preMatch;
              console.log(`Processing result for: ${item.filename}`);

              // 3. Check PDF Content Name (Priority 3)
              if (!currentMatch && labData.patientName) {
                const contentMatch = findBestMatch(labData.patientName, allPatients);
                if (contentMatch.score > 50) currentMatch = contentMatch; // Lower threshold for content match
              }

              // Default match if none found
              if (!currentMatch) currentMatch = { action: 'INBOX', patient: null, score: 0 };

              // Filter out metadata if it snuck into values
              if (labData.values["Sample Type"]) delete labData.values["Sample Type"];
              if (labData.values["Specimen"]) delete labData.values["Specimen"];
              
              // --- SANITIZATION (Fix Llama Hallucinations) ---
              // 1. Move Blood Group AND G6PD to staticUpdates if Llama put it in values
              const bgKeys = ["Blood Group", "Blood Group & Rh", "BG", "Blood Group and Rh"];
              bgKeys.forEach(k => {
                if (labData.values[k]) {
                  labData.staticUpdates["bloodGroup"] = labData.values[k];
                  delete labData.values[k];
                }
              });
              
              const g6pdKeys = ["G6PD", "G6PD Status", "Glucose-6-Phosphate Dehydrogenase"];
              g6pdKeys.forEach(k => {
                if (labData.values[k]) {
                  labData.staticUpdates["g6pd"] = labData.values[k];
                  delete labData.values[k];
                }
              });

              // 2. Remove Placeholder/Junk Keys
              const junkKeys = ["AnyOtherParam", "Other Param", "Value", "Parameter", "Test Name", "Result", "Observed Value"];
              junkKeys.forEach(k => delete labData.values[k]);
              
              // Extra safety: Remove keys containing "AnyOther" or "Placeholder"
              Object.keys(labData.values).forEach(k => {
                if (k.toLowerCase().includes("anyother") || k.toLowerCase().includes("placeholder")) delete labData.values[k];
              });
              
              // 3. CLEANUP VALUES (Remove Units & "Not Found")
              Object.keys(labData.values).forEach(k => {
                let val = labData.values[k];
                if (typeof val !== 'string') return;
                
                // A. Remove "Not Found", "Not Done", etc.
                const lower = val.toLowerCase().trim();
                const invalidPhrases = ["not found", "not done", "pending", "test not performed", "see below", "comment", "note", "not detected", "sample not received"];
                if (invalidPhrases.some(phrase => lower.includes(phrase)) || lower === "value") {
                  delete labData.values[k];
                  return;
                }

                // B. Remove Units from Numeric Fields (e.g. "12.5 g/dL" -> "12.5")
                // Only apply to fields that are typically single numbers (not combined like Na/K/Cl or text like CS)
                const NUMERIC_KEYS = ["Hb", "TLC", "Platelets", "CRP", "I. Ca", "NRBC", "APTT", "Creatinine", "SGPT"];
                if (NUMERIC_KEYS.includes(k)) {
                   // Replace anything that isn't a digit, dot, or comma
                   const cleaned = val.replace(/[^\d\.,]/g, '').trim();
                   // Only update if we actually have a number left (prevents deleting "Positive" for CRP if applicable)
                   if (cleaned.length > 0 && /\d/.test(cleaned)) {
                     labData.values[k] = cleaned;
                   }
                }
              });

              // --- ALIAS MAPPING (Fix for WBC -> TLC, etc.) ---
              const PARAM_ALIASES = {
                "WBC Count": "TLC", "Total WBC": "TLC", "WBC": "TLC", "Leukocyte Count": "TLC", "Total Leucocyte Count": "TLC", "T.L.C": "TLC",
                "Platelet Count": "Platelets", "PLT": "Platelets", "Platelet": "Platelets", "PLT Count": "Platelets",
                "Hemoglobin": "Hb", "HGB": "Hb", "Haemoglobin": "Hb",
                "Neutrophil Count": "Neutrophils", "Lymphocyte Count": "Lymphocytes"
              };
              
              const normalizedValues = {};
              Object.entries(labData.values).forEach(([k, v]) => {
                const mappedKey = PARAM_ALIASES[k.trim()] || k.trim();
                normalizedValues[mappedKey] = v;
              });
              labData.values = normalizedValues;

              // SPLIT PARAMETERS (Culture vs General vs New)
              const CULTURE_KEYS = ["Blood CS", "BAL CS", "Tip CS"];
              const GENERAL_KEYS = ["Hb", "TLC", "Platelets", "CRP", "Na/K/Cl", "I. Ca", "NRBC", "Sr.Bili(T/D)", "PT/INR", "APTT", "Creatinine", "SGPT", "POCUS", "Antibiotics", "Blood products", "Anti Apnea", "Inotropes"];
              // Ignore common Hemogram/Diff indices to prevent Inbox spam (User request: "in other than hemogram page")
              const IGNORED_KEYS = ["MCV", "MCH", "MCHC", "RDW", "PCV", "Hct", "Neutrophils", "Lymphocytes", "Monocytes", "Eosinophils", "Basophils", "MPV", "PDW", "PCT", "RBC", "RBC Count", "Mean Platelet Volume"];
              
              const generalValues = {};
              const cultureValues = {};
              const newValues = {};

              Object.entries(labData.values).forEach(([k, v]) => {
                if (CULTURE_KEYS.includes(k)) cultureValues[k] = v;
                else if (GENERAL_KEYS.includes(k)) generalValues[k] = v;
                else if (!IGNORED_KEYS.includes(k)) newValues[k] = v;
              });

              // DATE LOGIC: Use Collection Date for General, Report Date for Culture
              const collectionDate = labData.dates?.collection || new Date().toISOString().split('T')[0];
              const cultureDate = labData.dates?.report || collectionDate; 
              
              if (currentMatch.action === 'AUTO_SAVE' && !labData.forceInbox) {
                let savedTypes = [];
                try {
                  // 1. Auto-save General Parameters (using Collection Date)
                  if (Object.keys(generalValues).length > 0 || Object.keys(labData.staticUpdates).length > 0) {
                    console.log(`[AUTO-SAVE] General Params for ${currentMatch.patient.name}`);
                    saveToChart(currentMatch.patient.id, { ...labData, values: generalValues, reportDate: collectionDate });
                    savedTypes.push("General");
                  }
                  // 2. Auto-save Culture Parameters (using Report Date)
                  if (Object.keys(cultureValues).length > 0) {
                    console.log(`[AUTO-SAVE] Culture Params for ${currentMatch.patient.name}`);
                    saveToChart(currentMatch.patient.id, { ...labData, values: cultureValues, reportDate: cultureDate });
                    savedTypes.push("Culture");
                  }
                  
                  // LOG NOTIFICATION
                  if (savedTypes.length > 0) {
                    saveNotification({
                      patientName: currentMatch.patient.name,
                      type: 'AUTO_SAVE',
                      details: `Auto-saved: ${savedTypes.join(", ")}`,
                      timestamp: new Date().toISOString()
                    });
                  }

                  // 3. Send ONLY New Parameters to Inbox for approval
                  if (Object.keys(newValues).length > 0) {
                    console.log(`[INBOX] New Parameters for ${currentMatch.patient.name}`);
                    // Explicitly pass collectionDate as the report date for these parameters
                    saveToInbox({ ...labData, values: newValues, staticUpdates: {}, reportDate: collectionDate }, currentMatch, "New Parameters");
                  }
                } catch (e) {
                  console.error(`[AUTO-SAVE FAILED] ${e.message}. Redirecting all data to Inbox.`);
                  // Fallback: Send EVERYTHING to Inbox so user can save manually
                  saveToInbox({ ...labData, reportDate: collectionDate }, currentMatch, "Auto-save Failed");
                }
              } else {
                const reason = labData.forceInbox ? "Sample Type (Fluid/Tissue)" : "Low Score";
                console.log(`[INBOX] ${currentMatch.patient ? currentMatch.patient.name : 'Unknown'} (Reason: ${reason})`);
                // Ensure reportDate is top-level for the Inbox
                saveToInbox({ ...labData, reportDate: collectionDate }, currentMatch, reason);
                
                saveNotification({
                  patientName: currentMatch.patient ? currentMatch.patient.name : (labData.patientName || "Unknown"),
                  type: 'INBOX',
                  details: `Sent to Inbox: ${reason}`,
                  timestamp: new Date().toISOString()
                });
              }
              
              // Mark thread for labeling
              batchThreads.add(item.thread);
        }
      }

      // NEW: Label threads IMMEDIATELY after batch processing (Prevents duplicates on timeout)
      batchThreads.forEach(t => {
        t.addLabel(label);
        if (oldLabel) t.removeLabel(oldLabel);
      });

    } catch (e) {
      console.error("Batch Error:", e);
    }
    
    // Pause between batches to be kind to the API
    if (i + GEMINI_BATCH_SIZE < pdfQueue.length) Utilities.sleep(30000);
  }
  
  } catch (e) {
    console.error("Critical Execution Error:", e);
  } finally {
    lock.releaseLock();
  }
}

// ==========================================
// 1. GEMINI BATCH API (2.5 FLASH)
// ==========================================
function analyzeBatchWithGemini(batchItems, userInstructions) {
  const url = `https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent?key=${CONFIG.geminiApiKey}`;
  
  const prompt = `
    You are a medical data assistant. I have attached ${batchItems.length} PDF lab reports.
    Return a JSON ARRAY containing exactly ${batchItems.length} objects.
    
    For EACH report, extract data into this structure:
    {
      "filename": "The exact filename provided",
      "patientName": "Name of patient",
      "dates": {
        "collection": "YYYY-MM-DD", 
        "report": "YYYY-MM-DD"
      },
      "forceInbox": boolean, // True for Fluid/Tissue samples. False for Blood/Serum/Plasma/BAL/Tip.
      "values": {
        "Hb": "val", "TLC": "val", "Platelets": "val", "CRP": "val",
        "Na/K/Cl": "Na / K / Cl", "I. Ca": "val", "NRBC": "val",
        "Sr.Bili(T/D)": "Total / Direct", "PT/INR": "PT / INR",
        "APTT": "val", "Creatinine": "val", "SGPT": "val",
        "Blood CS": "Organism or 'No growth' or 'No growth (interim)'", "BAL CS": "Organism", "Tip CS": "Type - Organism", "POCUS": "Findings"
      },
      "staticUpdates": {
        "bloodGroup": "e.g. O +ve",
        "g6pd": "Normal/Deficient"
      }
    }
    Rules:
    1. STRICTLY JSON ONLY.
    2. EXTRACT NUMBERS ONLY for quantitative tests. Do NOT include units (e.g. extract "12.5", NOT "12.5 g/dL").
    3. Map 'WBC'->'TLC', 'HGB'->'Hb', 'PLT'->'Platelets'.
    4. 'Blood Group' MUST go to 'staticUpdates'.
    5. Ignore missing/pending values. No placeholder keys.
    6. Culture Reports: If 'No Growth', use exactly "No growth". If interim (e.g. 48h no growth), use "No growth (interim)".
    ${userInstructions ? "\n    7. SPECIAL USER INSTRUCTIONS (OVERRIDE RULES):\n    " + userInstructions : ""}
  `;

  // Build Multipart Request
  const parts = [{ text: prompt }];
  
  batchItems.forEach((item) => {
    parts.push({ text: `\n--- FILE: ${item.filename} ---\n` });
    parts.push({
      inlineData: {
        mimeType: "application/pdf",
        data: Utilities.base64Encode(item.blob.getBytes())
      }
    });
  });

  const payload = {
    contents: [{ role: "user", parts: parts }],
    generationConfig: { responseMimeType: "application/json" }
  };

  const options = {
    method: "post",
    contentType: "application/json",
    payload: JSON.stringify(payload),
    muteHttpExceptions: true
  };

  const response = UrlFetchApp.fetch(url, options);
  const json = JSON.parse(response.getContentText());

  if (json.error) throw new Error(json.error.message);
  return JSON.parse(json.candidates[0].content.parts[0].text);
}

// ==========================================
// 3. INTELLIGENT MATCHING
// ==========================================
function findBestMatch(labName, patients) {
  const lab = normalizeName(labName);
  console.log(`[MATCH DEBUG] Lab Name: "${labName}" -> Normalized: "${lab.name}" (Ordinal: ${lab.ordinal})`);

  let bestMatch = null;
  let maxScore = 0;
  for (const p of patients) {
    const app = normalizeName(p.name);
    if (lab.ordinal !== app.ordinal) continue; 
    const score = calculateSimilarityScore(lab.name, app.name);
    
    // Log close matches to help debug
    if (score > 50) {
      console.log(`[MATCH DEBUG] Candidate: "${p.name}" -> Norm: "${app.name}" | Score: ${score}`);
    }

    if (score > maxScore) { maxScore = score; bestMatch = p; }
  }
  if (bestMatch && maxScore >= 75) return { action: 'AUTO_SAVE', patient: bestMatch, score: maxScore };
  else return { action: 'INBOX', patient: bestMatch, score: maxScore };
}

function normalizeName(raw) {
  if (!raw) return { name: "", ordinal: null };
  let s = raw.trim().toLowerCase();
  
  // 1. Remove common noise (Fix for Subject Lines)
  s = s.replace(/laboratory report/g, '');

  let ordinal = null;
  const map = {'first':1,'1st':1,'(1)':1,'second':2,'2nd':2,'(2)':2,'third':3,'3rd':3,'(3)':3};
  for(const k in map) { if(s.includes(k)) { ordinal = map[k]; s = s.replace(k,''); break; } }
  
  // 2. Remove prefixes (Handle B/O specifically before stripping symbols)
  s = s.replace(/b\/o/g, ''); 
  s = s.replace(/baby of/g, '');
  s = s.replace(/\b(baby|mast|miss)\b/g, '');

  // 3. Remove non-alpha characters (replace with space to prevent merging words)
  s = s.replace(/[^a-z\s]/g, ' ');
  
  return { name: s.replace(/\s+/g, ' ').trim(), ordinal: ordinal };
}

function calculateSimilarityScore(name1, name2) {
  let parts1 = name1.split(/\s+/).filter(p => p);
  let parts2 = name2.split(/\s+/).filter(p => p);
  let str1 = name1, str2 = name2;
  if (parts1.length === 3 && parts2.length === 2) str1 = parts1[0] + " " + parts1[2];
  else if (parts2.length === 3 && parts1.length === 2) str2 = parts2[0] + " " + parts2[2];
  const dist = levenshtein(str1, str2);
  const maxLen = Math.max(str1.length, str2.length);
  if (maxLen === 0) return 100;
  return (1 - dist / maxLen) * 100;
}

function levenshtein(a, b) {
  const matrix = [];
  for (let i = 0; i <= b.length; i++) matrix[i] = [i];
  for (let j = 0; j <= a.length; j++) matrix[0][j] = j;
  for (let i = 1; i <= b.length; i++) {
    for (let j = 1; j <= a.length; j++) {
      if (b.charAt(i - 1) == a.charAt(j - 1)) matrix[i][j] = matrix[i - 1][j - 1];
      else matrix[i][j] = Math.min(matrix[i - 1][j - 1] + 1, Math.min(matrix[i][j - 1] + 1, matrix[i - 1][j] + 1));
    }
  }
  return matrix[b.length][a.length];
}

// ==========================================
// 4. FIRESTORE ACTIONS (FIXED REFERENCE ERROR)
// ==========================================
function saveToInbox(labData, matchResult, reason) {
  const payload = {
    patientName: labData.patientName,
    receivedAt: new Date().toISOString(),
    reportDate: labData.reportDate, // Pass the specific date context to frontend
    data: labData.values,
    staticUpdates: labData.staticUpdates,
    suggestedMatchId: matchResult.patient ? matchResult.patient.id : null,
    matchScore: matchResult.score,
    status: "Pending",
    reason: reason
  };
  firestore.createDocument(CONFIG.pathInbox, payload);
}

function saveNotification(note) {
  try { firestore.createDocument(CONFIG.pathNotifications, note); } catch(e) { console.error("Failed to save notification", e); }
}

function saveToChart(patientId, labData) {
  const docPath = `${CONFIG.pathCharts}/${patientId}`;
  
  // Standard Rows Definition (Must match Frontend)
  const DEFAULT_ROWS = [
    { label: "Hb", category: "Investigations", data: {} },
    { label: "TLC", category: "Investigations", data: {} },
    { label: "Platelets", category: "Investigations", data: {} },
    { label: "CRP", category: "Investigations", data: {} },
    { label: "Na/K/Cl", category: "Investigations", data: {} }
  ];

  let chart;
  let isNew = false;

  // 1. READ & PARSE EXISTING DATA (Handle Raw Firestore JSON)
  let dates = [];
  let rows = [];
  let staticData = {};

  try { 
    const doc = firestore.getDocument(docPath);
    chart = doc.fields; // Raw Firestore fields
    
    // Parse Dates
    if (chart.dates?.arrayValue?.values) {
      dates = chart.dates.arrayValue.values.map(v => v.stringValue);
    }
    
    // Parse Rows
    if (chart.rows?.arrayValue?.values) {
      rows = chart.rows.arrayValue.values.map(row => {
        const r = row.mapValue.fields;
        const dataMap = {};
        if (r.data?.mapValue?.fields) {
          for (const [k, v] of Object.entries(r.data.mapValue.fields)) dataMap[k] = v.stringValue;
        }
        return { label: r.label.stringValue, category: r.category.stringValue, data: dataMap };
      });
    }

    // Parse Static
    if (chart.static?.mapValue?.fields) {
      for (const [k, v] of Object.entries(chart.static.mapValue.fields)) staticData[k] = v.stringValue;
    }

  } catch(e) { 
    // Document doesn't exist, start fresh
    isNew = true;
    rows = JSON.parse(JSON.stringify(DEFAULT_ROWS)); // Initialize with standard rows
  }
  
  // 2. SMART DATE RESOLUTION (Handle Collisions)
  // Logic: If data exists for a parameter on the target date, create a new column (e.g. "Date (2)")
  let targetDate = labData.reportDate;
  let finalDateKey = targetDate;

  // Helper: Check if any of the NEW values conflict with EXISTING values in a specific column
  const hasConflict = (colName) => {
    return Object.keys(labData.values).some(key => {
      const row = rows.find(r => r.label === key);
      // Conflict exists if the row exists AND has data for this column AND that data is not empty
      const existing = row && row.data ? row.data[colName] : null;
      return existing && existing.trim() !== "" && existing !== labData.values[key];
    });
  };

  if (dates.includes(targetDate) && hasConflict(targetDate)) {
    // Conflict found! Try to find the next available slot (e.g. "2023-10-27 (2)")
    let suffix = 2;
    while (true) {
      const candidate = `${targetDate} (${suffix})`;
      // If column doesn't exist, or exists but has no conflicts for THESE specific parameters -> Use it
      if (!dates.includes(candidate) || !hasConflict(candidate)) {
        finalDateKey = candidate;
        break;
      }
      suffix++;
    }
  }

  // Add the resolved date key if it's new
  if (!dates.includes(finalDateKey) && Object.keys(labData.values).length > 0) {
    dates.push(finalDateKey);
    dates.sort();
  }

  for (const [key, value] of Object.entries(labData.values)) {
    let row = rows.find(r => r.label === key);
    if (!row) {
      row = { label: key, category: "Investigations", data: {} };
      rows.push(row);
    }
    row.data[finalDateKey] = value;
  }

  // Update Static Fields
  for (const [key, value] of Object.entries(labData.staticUpdates)) {
     // Request #1: Only update if value is present and not empty/dash
     if (value && value.trim() !== "" && value !== "-") {
       staticData[key] = value;
     }
  }

  // 3. SAVE PAYLOAD (Library handles serialization)
  const payload = {
    dates: dates,
    rows: rows,
    static: staticData
  };

  // Use updateDocument (which usually acts as PATCH/UPSERT in this library)
  firestore.updateDocument(docPath, payload);
}

// ==========================================
// DEBUG: CHECK AVAILABLE MODELS
// ==========================================
function listGeminiModels() {
  const url = `https://generativelanguage.googleapis.com/v1beta/models?key=${CONFIG.geminiApiKey}`;
  const response = UrlFetchApp.fetch(url, { muteHttpExceptions: true });
  const json = JSON.parse(response.getContentText());

  if (json.error) {
    console.error("API Error:", json.error);
    return;
  }

  console.log("--- Available Models for your API Key ---");
  const models = json.models || [];
  models.forEach(m => {
    // Only show models that support text generation
    if (m.supportedGenerationMethods && m.supportedGenerationMethods.includes("generateContent")) {
      console.log(m.name);
    }
  });
}