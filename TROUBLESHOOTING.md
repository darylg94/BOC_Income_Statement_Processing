# Troubleshooting Guide

## ✅ Issue Fixed: "Unknown function PARSE_FINANCIAL_REPORT"

### What Was Wrong
The stored procedure `PROCESS_INCOME_STATEMENT` was trying to call other procedures using function-style syntax, which doesn't work in Snowflake's SQL stored procedures.

### What Was Fixed
Changed from:
```sql
parse_result := PARSE_FINANCIAL_REPORT(FILE_PATH, REPORT_YEAR, REPORT_PERIOD);
```

To:
```sql
CALL PARSE_FINANCIAL_REPORT(FILE_PATH, REPORT_YEAR, REPORT_PERIOD) INTO parse_result;
```

### Next Steps
1. **Re-run the setup** to recreate the procedures:
   ```sql
   @income_statement_pipeline.sql
   ```

2. **Test with a single file**:
   ```sql
   CALL PROCESS_INCOME_STATEMENT(
       '2023 Final Results EN 14Mar24.pdf',
       2023,
       'FY2023'
   );
   ```

3. **Or use the fixed run script**:
   ```sql
   @run_pipeline_fixed.sql
   ```

---

## Common Issues & Solutions

### 1. Stage Not Found / Wrong Stage Name

**Error**: `Stage 'FINANCIAL_REPORTS_STAGE' does not exist`

**Issue**: Your actual stage is called `Documents`

**Solution**: 
- Use `@Documents` instead of `@FINANCIAL_REPORTS_STAGE`
- Or recreate the stage with the expected name:
  ```sql
  CREATE OR REPLACE STAGE FINANCIAL_REPORTS_STAGE
      DIRECTORY = (ENABLE = TRUE)
      ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE');
  ```

### 2. Database Name Mismatch

**Your database**: `BOCA_INCOME_STATEMENT`
**Automation scripts expect**: `BOC_INCOME_STATEMENT`

**Solution**: Either:
- Update automation scripts to use `BOCA_INCOME_STATEMENT`, OR
- Rename your database:
  ```sql
  ALTER DATABASE BOCA_INCOME_STATEMENT RENAME TO BOC_INCOME_STATEMENT;
  ```

### 3. Procedure Not Found

**Error**: `Procedure 'XXX' does not exist`

**Check**:
```sql
SHOW PROCEDURES IN BOCA_INCOME_STATEMENT;
```

**Solution**: Re-run the pipeline setup:
```sql
@income_statement_pipeline.sql
```

### 4. File Not Found in Stage

**Error**: `File not found: 'filename.pdf'`

**Check**:
```sql
LIST @Documents;
```

**Solution**: Upload the file:
```sql
PUT file:///path/to/file.pdf @Documents AUTO_COMPRESS=FALSE;
```

### 5. AI_PARSE_DOCUMENT Error

**Error**: `Invalid identifier 'AI_PARSE_DOCUMENT'`

**Cause**: Document AI not enabled or wrong Snowflake edition

**Solution**:
- Ensure you're using Snowflake Enterprise or higher
- Contact your Snowflake account team to enable Document AI
- Or use the trial: https://signup.snowflake.com/

### 6. Parsing Takes Too Long / Timeout

**Issue**: Large PDF files timing out

**Solution**:
- Use a larger warehouse:
  ```sql
  USE WAREHOUSE LARGE_WH;
  ```
- Or increase statement timeout:
  ```sql
  ALTER SESSION SET STATEMENT_TIMEOUT_IN_SECONDS = 3600;
  ```

### 7. Extraction Returns NULL Values

**Issue**: `AI_EXTRACT` not finding the data

**Cause**: Income statement format differs from expected

**Debug**:
```sql
-- Check the raw parsed content
SELECT parsed_content
FROM RAW.PARSED_DOCUMENTS
WHERE document_id = 'DOC_2023_FY2023';
```

**Solution**: Adjust the extraction prompts in `EXTRACT_INCOME_STATEMENT` procedure

### 8. Dynamic Table Not Refreshing

**Check**:
```sql
SELECT * FROM TABLE(INFORMATION_SCHEMA.DYNAMIC_TABLES())
WHERE TABLE_NAME = 'INCOME_STATEMENT_TRENDS';
```

**Solution**: Manually refresh:
```sql
ALTER DYNAMIC TABLE PROCESSED.INCOME_STATEMENT_TRENDS REFRESH;
```

### 9. Task Not Running (For Automation)

**Check task status**:
```sql
SHOW TASKS IN BOCA_INCOME_STATEMENT;
```

**If suspended**:
```sql
ALTER TASK TASK_DISCOVER_FILES RESUME;
ALTER TASK TASK_PROCESS_FILES RESUME;
```

### 10. Permission Denied

**Error**: `Insufficient privileges`

**Check**:
```sql
SHOW GRANTS TO USER CURRENT_USER();
```

**Solution**: Grant necessary permissions:
```sql
GRANT USAGE ON WAREHOUSE COMPUTE_WH TO ROLE YOUR_ROLE;
GRANT ALL ON DATABASE BOCA_INCOME_STATEMENT TO ROLE YOUR_ROLE;
```

---

## Verification Checklist

Before processing files, verify:

- [ ] Database exists: `BOCA_INCOME_STATEMENT`
- [ ] Schemas exist: `RAW` and `PROCESSED`
- [ ] Stage exists: `Documents` (with directory enabled)
- [ ] Procedures exist: Run `SHOW PROCEDURES;`
- [ ] Tables exist: Run `SHOW TABLES IN RAW;` and `SHOW TABLES IN PROCESSED;`
- [ ] Files are in stage: `LIST @Documents;`
- [ ] Warehouse is active: `SHOW WAREHOUSES;`

---

## Quick Test

Use this test script to verify everything:

```sql
@test_pipeline.sql
```

---

## Step-by-Step Recovery

If nothing works, start from scratch:

```sql
-- 1. Drop everything
DROP DATABASE IF EXISTS BOCA_INCOME_STATEMENT CASCADE;

-- 2. Recreate from base
@income_statement_pipeline.sql

-- 3. Upload a test file
PUT file:///path/to/test.pdf @Documents;

-- 4. Process it
CALL PROCESS_INCOME_STATEMENT('test.pdf', 2024, 'FY2024');

-- 5. Check results
SELECT * FROM PROCESSED.INCOME_STATEMENT;
```

---

## Getting Help

1. **Check procedure logs**: Look at query history in Snowflake UI
2. **View error details**: 
   ```sql
   SELECT * FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY())
   WHERE ERROR_MESSAGE IS NOT NULL
   ORDER BY START_TIME DESC
   LIMIT 10;
   ```
3. **Test components separately**:
   - Test OCR: `CALL PARSE_FINANCIAL_REPORT(...);`
   - Test extraction: `CALL EXTRACT_INCOME_STATEMENT(...);`
   - Test full flow: `CALL PROCESS_INCOME_STATEMENT(...);`

---

## Contact Information

For Snowflake Document AI issues:
- Documentation: https://docs.snowflake.com/en/user-guide/snowflake-cortex/document-ai
- Support: Contact your Snowflake account team

