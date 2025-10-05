-- =====================================================
-- Execute Income Statement Pipeline (FIXED VERSION)
-- =====================================================

USE WAREHOUSE COMPUTE_WH;
USE DATABASE BOCA_INCOME_STATEMENT;

-- =====================================================
-- STEP 1: Upload PDF Reports to Stage
-- =====================================================



-- Verify files uploaded
LIST @Documents;

-- =====================================================
-- STEP 2: Process Reports
-- =====================================================



-- Process 2024 Final Results
CALL PROCESS_INCOME_STATEMENT(
    '2024 Final Results Announcement EN 20250313.pdf',
    2024,
    'FY2024'
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
-- STEP 4: View Results & Analysis
-- =====================================================

SELECT '=== STEP 4: Viewing Results ===' as step;

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
    ROUND((lease_rental_income / NULLIF(total_revenue, 0) * 100), 1) as lease_rental_pct,
    ROUND((net_gain_disposal_aircraft / NULLIF(total_revenue, 0) * 100), 1) as disposal_gain_pct,
    ROUND((other_income / NULLIF(total_revenue, 0) * 100), 1) as other_income_pct
FROM PROCESSED.INCOME_STATEMENT
WHERE total_revenue IS NOT NULL
ORDER BY report_year DESC;

-- 2. Profitability Trend
SELECT 
    report_period,
    total_revenue,
    operating_profit,
    profit_after_tax,
    ROUND((operating_profit / NULLIF(total_revenue, 0) * 100), 2) as operating_margin,
    ROUND((profit_after_tax / NULLIF(total_revenue, 0) * 100), 2) as net_margin,
    basic_earnings_per_share
FROM PROCESSED.INCOME_STATEMENT
WHERE total_revenue IS NOT NULL
ORDER BY report_year DESC;

-- 3. Year-over-Year Growth
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



