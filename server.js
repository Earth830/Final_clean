const express = require("express");
const { google } = require("googleapis");
const cors = require("cors");
const app = express();
app.use(cors());

const PORT = 3001;
const RANGE = "Sheet!A1:Z2000";

// ไฟล์ Service Account (ใช้ forward-slash หรือ escape backslash)
const SERVICE_ACCOUNT_FILE_1 = "C:/Users/Asus/OneDrive/เอกสาร/Jobber/SSL/Python Code/Search_products/search-products-459304-16109be1f71e.json";
const SERVICE_ACCOUNT_FILE_2 = "C:\Users\Asus\OneDrive\เอกสาร\Jobber\SSL\Python Code\Search_products\songsod02-f0faffe8b254.json";

// ใช้สอง Spreadsheet IDs
const SPREADSHEET_ID_1 = "124bHRo_xyV39gUytA-TXoWCbvwGPNuZ1oDtVq8gCBIY";
const SPREADSHEET_ID_2 = "1x-3hsUZ77Fx8sl_cdaFIBvdbYJDPhKQfd3VfWUnbT48";

// ฟังก์ชันดึงข้อมูลจาก Google Sheets รับไฟล์และ ID มาด้วย
async function getSheetData(serviceAccountFile, spreadsheetId) {
  const auth = new google.auth.GoogleAuth({
    keyFile: serviceAccountFile,
    scopes: ["https://www.googleapis.com/auth/spreadsheets.readonly"],
  });
  const client = await auth.getClient();
  const sheets = google.sheets({ version: "v4", auth: client });

  const res = await sheets.spreadsheets.values.get({
    spreadsheetId,
    range: RANGE,
  });
  return res.data.values;
}

// Middleware ตรวจสอบ Service Account
function checkServiceAccount(req, res, next) {
  const svc = req.query.service_account || req.headers["service_account"];
  if (svc !== "account1" && svc !== "account2") {
    return res.status(403).json({ error: "Forbidden - Invalid Service Account" });
  }
  next();
}

// Route ใช้สองไฟล์ JSON และสอง Sheet IDs
app.get("/products", checkServiceAccount, async (req, res) => {
  try {
    const svc = req.query.service_account || req.headers["service_account"];
    let rows;

    if (svc === "account1") {
      rows = await getSheetData(SERVICE_ACCOUNT_FILE_1, SPREADSHEET_ID_1);
    } else {
      rows = await getSheetData(SERVICE_ACCOUNT_FILE_2, SPREADSHEET_ID_2);
    }

    if (!rows || rows.length === 0) return res.json([]);

    const headers = rows[0];
    const dataRows = rows.slice(1);
    const products = dataRows.map(row => {
      const obj = {};
      headers.forEach((h, i) => obj[h] = row[i] || "");
      return obj;
    });

    res.json(products);
  } catch (error) {
    console.error(error);
    res.status(500).send("Error fetching data");
  }
});

app.listen(PORT, () => {
  console.log(`Backend server running at http://localhost:${PORT}`);
});
