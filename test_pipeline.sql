-- =====================================================
-- TEST SCRIPT: Verify Pipeline Setup
-- =====================================================

USE WAREHOUSE COMPUTE_WH;
USE DATABASE BOCA_INCOME_STATEMENT;

-- Step 1: Verify database and schemas exist
SELECT '=== STEP 1: Verify Database Setup ===' as test_step;

SELECT CURRENT_DATABASE() as current_db;
SHOW SCHEMAS;

-- Step 2: Verify stage exists
SELECT '=== STEP 2: Verify Stage Setup ===' as test_step;

SHOW STAGES;

-- Step 3: Verify tables exist
SELECT '=== STEP 3: Verify Tables ===' as test_step;

SHOW TABLES IN RAW;
SHOW TABLES IN PROCESSED;

-- Step 4: Verify procedures exist
SELECT '=== STEP 4: Verify Procedures ===' as test_step;

SHOW PROCEDURES LIKE 'PARSE_FINANCIAL_REPORT';
SHOW PROCEDURES LIKE 'EXTRACT_INCOME_STATEMENT';
SHOW PROCEDURES LIKE 'PROCESS_INCOME_STATEMENT';

-- Step 5: Check if files are in stage
SELECT '=== STEP 5: Check Stage Files ===' as test_step;

LIST @Documents;

-- Step 6: Test processing a single file
SELECT '=== STEP 6: Test Processing (if files exist) ===' as test_step;

-- Uncomment and modify the filename below to test
-- CALL PROCESS_INCOME_STATEMENT(
--     '2023 Final Results EN 14Mar24.pdf',
--     2023,
--     'FY2023'
-- );

-- Step 7: Check results (if processing was run)
SELECT '=== STEP 7: Check Results ===' as test_step;

-- Check parsed documents
SELECT 
    document_id,
    file_name,
    report_year,
    report_period,
    parsed_at
FROM RAW.PARSED_DOCUMENTS
ORDER BY parsed_at DESC;

-- Check extracted income statements
SELECT 
    statement_id,
    report_period,
    report_year,
    total_revenue,
    operating_profit,
    profit_after_tax,
    basic_earnings_per_share
FROM PROCESSED.INCOME_STATEMENT
ORDER BY report_year DESC;

-- Check dynamic table
SELECT * FROM PROCESSED.INCOME_STATEMENT_TRENDS
ORDER BY report_year DESC;

SELECT '=== TEST COMPLETE ===' as status;

