# 🎯 START HERE - Income Statement Pipeline

## ✅ ISSUE FIXED!

The error **"Unknown function PARSE_FINANCIAL_REPORT"** has been resolved in the updated `income_statement_pipeline.sql` file.

---

## 🚀 Next Steps (Choose Your Path)

### Path A: Quick Test (Recommended)

Run this to verify everything works:

```sql
@test_pipeline.sql
```

Then process one file:

```sql
USE DATABASE BOCA_INCOME_STATEMENT;

CALL RAW.PROCESS_INCOME_STATEMENT(
    '2023 Final Results EN 14Mar24.pdf',
    2023,
    'FY2023'
);
```

### Path B: Process All Files

Run the complete pipeline:

```sql
@run_pipeline_fixed.sql
```

This will:
1. Upload all PDFs from your reports folder
2. Process each one automatically
3. Show results and analytics

---

## 📁 Your Current Setup

| Item | Name |
|------|------|
| **Database** | `BOCA_INCOME_STATEMENT` |
| **Stage** | `@Documents` |
| **Warehouse** | `COMPUTE_WH` |

---

## 🔄 What Was Fixed

### Before (Broken):
```sql
parse_result := PARSE_FINANCIAL_REPORT(FILE_PATH, REPORT_YEAR, REPORT_PERIOD);
```

### After (Fixed):
```sql
CALL RAW.PARSE_FINANCIAL_REPORT(FILE_PATH, REPORT_YEAR, REPORT_PERIOD) INTO parse_result;
```

**Why**: In Snowflake, stored procedures must call other procedures using `CALL ... INTO`, not function-style syntax.

---

## 📋 File Guide

| File | Purpose | When to Use |
|------|---------|-------------|
| **`income_statement_pipeline.sql`** | ✅ **UPDATED** - Base setup | Already has the fix |
| **`run_pipeline_fixed.sql`** | Process all your PDFs | Ready to run now |
| **`test_pipeline.sql`** | Verify setup works | Test first |
| **`TROUBLESHOOTING.md`** | Fix common issues | If errors occur |

---

## ⚡ Quick Commands

### Check if procedures exist
```sql
USE DATABASE BOCA_INCOME_STATEMENT;
SHOW PROCEDURES IN SCHEMA RAW;
```

### Check if files are uploaded:
```sql
LIST @Documents;
```

### Upload files:
```sql
PUT file:///Users/dgoh/Desktop/Customer\ PoCs/BOC\ Aviation/pdfs/reports/*.pdf 
    @Documents 
    AUTO_COMPRESS=FALSE;
```

### Process a single file:
```sql
CALL RAW.PROCESS_INCOME_STATEMENT(
    '2023 Final Results EN 14Mar24.pdf',
    2023,
    'FY2023'
);
```

### View results:
```sql
SELECT * FROM PROCESSED.INCOME_STATEMENT_SUMMARY;
```

---

## 🎯 Success Path

1. ✅ **Fixed** - Updated procedure syntax
2. 🔄 **Next** - Test with one file
3. 📊 **Then** - Process all files
4. 🎉 **Done** - Query your data!

---

## ❓ If You Get Errors

1. **Check**: `TROUBLESHOOTING.md`
2. **Verify**: Run `test_pipeline.sql`
3. **Debug**: Check the specific error message

---

## 💡 Pro Tips

1. **Test first**: Process one file before batch processing
2. **Check results**: Query `PROCESSED.INCOME_STATEMENT` after each run
3. **Monitor warehouse**: Use `COMPUTE_WH` or larger if needed
4. **Save queries**: Bookmark your favorite analysis queries

---

## 📊 What You'll Get

After processing, you'll have:

### Tables with Data:
- ✅ `RAW.PARSED_DOCUMENTS` - OCR extracted text
- ✅ `PROCESSED.INCOME_STATEMENT` - Structured financial data

### Ready-to-Use Views:
- ✅ `PROCESSED.INCOME_STATEMENT_SUMMARY` - Key metrics & growth
- ✅ `PROCESSED.INCOME_STATEMENT_DETAILED` - Full line items
- ✅ `PROCESSED.INCOME_STATEMENT_TRENDS` - YoY comparisons

### Data You Can Query:
- Revenue (total, by type)
- Operating profit
- Net profit
- EPS (basic & diluted)
- Margins (operating, net)
- Growth rates (YoY)

---

## 🎯 Recommended Flow

```sql
-- Step 1: Test the setup
@test_pipeline.sql

-- Step 2: Upload files if needed
PUT file:///path/to/*.pdf @Documents;

-- Step 3: Process one file
CALL RAW.PROCESS_INCOME_STATEMENT(
    'your_file.pdf',
    2024,
    'FY2024'
);

-- Step 4: Check results
SELECT * FROM PROCESSED.INCOME_STATEMENT_SUMMARY;

-- Step 5: If successful, process remaining files
@run_pipeline_fixed.sql
```

---

## ✅ You're Ready!

The fix is in place. Just run the test script or start processing files!

**First command to run**:
```sql
@test_pipeline.sql
```

Good luck! 🚀

