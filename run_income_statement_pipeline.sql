-- =====================================================
-- Execute Income Statement Pipeline
-- =====================================================

USE WAREHOUSE COMPUTE_WH;
USE DATABASE BOC_INCOME_STATEMENT;

-- =====================================================
-- STEP 1: Upload PDF Reports to Stage
-- =====================================================

PUT file:///Users/dgoh/Desktop/Customer\ PoCs/BOC\ Aviation/pdfs/reports/*.pdf 
    @FINANCIAL_REPORTS_STAGE 
    AUTO_COMPRESS=FALSE
    OVERWRITE=TRUE;

-- Verify files uploaded
LIST @FINANCIAL_REPORTS_STAGE;

-- =====================================================
-- STEP 2: Process Reports
-- =====================================================

-- Process 2024 Final Results
CALL PROCESS_INCOME_STATEMENT(
    '2024 Final Results Announcement EN 20250313.pdf',
    2024,
    'FY2024'
);

-- Process 2023 Final Results
CALL PROCESS_INCOME_STATEMENT(
    '2023 Final Results EN 14Mar24.pdf',
    2023,
    'FY2023'
);

-- =====================================================
-- STEP 3: Verify Data
-- =====================================================

-- Check parsed documents
SELECT 
    document_id,
    file_name,
    report_year,
    report_period,
    parsed_at
FROM RAW.PARSED_DOCUMENTS
ORDER BY report_year DESC;

-- View extracted income statements
SELECT 
    statement_id,
    report_period,
    report_year,
    total_revenue,
    operating_profit,
    profit_after_tax,
    basic_earnings_per_share,
    processed_at
FROM PROCESSED.INCOME_STATEMENT
ORDER BY report_year DESC;

-- =====================================================
-- STEP 4: View Results
-- =====================================================

-- Summary Dashboard
SELECT 
    report_year,
    report_period,
    total_revenue,
    operating_profit,
    profit_after_tax,
    profit_attributable_to_shareholders,
    basic_earnings_per_share,
    operating_margin_pct,
    net_margin_pct,
    revenue_growth_pct,
    profit_growth_pct
FROM PROCESSED.INCOME_STATEMENT_SUMMARY;

-- Detailed Income Statement
SELECT * FROM PROCESSED.INCOME_STATEMENT_DETAILED;

-- =====================================================
-- STEP 5: Analysis Queries
-- =====================================================

-- 1. Revenue Composition Analysis
SELECT 
    report_period,
    total_revenue,
    lease_rental_income,
    net_gain_disposal_aircraft,
    other_income,
    ROUND((lease_rental_income / total_revenue * 100), 1) as lease_rental_pct,
    ROUND((net_gain_disposal_aircraft / total_revenue * 100), 1) as disposal_gain_pct,
    ROUND((other_income / total_revenue * 100), 1) as other_income_pct
FROM PROCESSED.INCOME_STATEMENT
WHERE total_revenue IS NOT NULL
ORDER BY report_year DESC;

-- 2. Operating Expense Analysis
SELECT 
    report_period,
    depreciation,
    staff_costs,
    other_operating_expenses,
    (depreciation + staff_costs + other_operating_expenses) as total_expenses,
    operating_profit,
    ROUND((depreciation / NULLIF(total_revenue, 0) * 100), 1) as depreciation_pct,
    ROUND((staff_costs / NULLIF(total_revenue, 0) * 100), 1) as staff_costs_pct
FROM PROCESSED.INCOME_STATEMENT
WHERE total_revenue IS NOT NULL
ORDER BY report_year DESC;

-- 3. Profitability Trend
SELECT 
    report_period,
    total_revenue,
    operating_profit,
    profit_before_tax,
    profit_after_tax,
    ROUND((operating_profit / NULLIF(total_revenue, 0) * 100), 2) as operating_margin,
    ROUND((profit_after_tax / NULLIF(total_revenue, 0) * 100), 2) as net_margin,
    basic_earnings_per_share
FROM PROCESSED.INCOME_STATEMENT
WHERE total_revenue IS NOT NULL
ORDER BY report_year DESC;

-- 4. Year-over-Year Growth
SELECT 
    report_year,
    report_period,
    total_revenue,
    prior_period_revenue,
    revenue_growth_pct,
    profit_after_tax,
    prior_period_profit,
    profit_growth_pct
FROM PROCESSED.INCOME_STATEMENT_TRENDS
WHERE prior_period_revenue IS NOT NULL
ORDER BY report_year DESC;

-- 5. Tax Analysis
SELECT 
    report_period,
    profit_before_tax,
    income_tax_expense,
    profit_after_tax,
    ROUND((income_tax_expense / NULLIF(profit_before_tax, 0) * 100), 2) as effective_tax_rate_pct
FROM PROCESSED.INCOME_STATEMENT
WHERE profit_before_tax IS NOT NULL
ORDER BY report_year DESC;

-- =====================================================
-- STEP 6: Export for Reporting
-- =====================================================

-- Create simple CSV export view
CREATE OR REPLACE VIEW PROCESSED.INCOME_STATEMENT_EXPORT AS
SELECT 
    report_year,
    report_period,
    total_revenue as "Total Revenue (USD Million)",
    lease_rental_income as "Lease Rental Income (USD Million)",
    operating_profit as "Operating Profit (USD Million)",
    profit_before_tax as "Profit Before Tax (USD Million)",
    profit_after_tax as "Profit After Tax (USD Million)",
    profit_attributable_to_shareholders as "Profit to Shareholders (USD Million)",
    basic_earnings_per_share as "Basic EPS (USD)",
    ROUND((operating_profit / NULLIF(total_revenue, 0) * 100), 2) as "Operating Margin %",
    ROUND((profit_after_tax / NULLIF(total_revenue, 0) * 100), 2) as "Net Margin %"
FROM PROCESSED.INCOME_STATEMENT
ORDER BY report_year DESC;

-- View the export data
SELECT * FROM PROCESSED.INCOME_STATEMENT_EXPORT;

-- =====================================================
-- STEP 7: Monitor Pipeline
-- =====================================================

-- Check stream status
SELECT SYSTEM$STREAM_HAS_DATA('INCOME_STATEMENT_STREAM');

-- View stream changes
SELECT * FROM INCOME_STATEMENT_STREAM;

-- Check task execution
SELECT *
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY())
WHERE NAME = 'REFRESH_INCOME_STATEMENT_TRENDS'
ORDER BY SCHEDULED_TIME DESC
LIMIT 5;

-- Check dynamic table refresh
SELECT 
    TABLE_NAME,
    TARGET_LAG,
    DATA_TIMESTAMP,
    REFRESH_MODE,
    LAST_REFRESH_START_TIME,
    LAST_REFRESH_END_TIME
FROM TABLE(INFORMATION_SCHEMA.DYNAMIC_TABLES())
WHERE TABLE_NAME = 'INCOME_STATEMENT_TRENDS';
