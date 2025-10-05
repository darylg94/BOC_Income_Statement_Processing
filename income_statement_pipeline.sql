-- =====================================================
-- BOC Aviation Income Statement Pipeline
-- Simple workflow using Document AI for OCR extraction
-- =====================================================
CREATE DATABASE IF NOT EXISTS BOCA_INCOME_STATEMENT;
USE DATABASE BOCA_INCOME_STATEMENT;

CREATE SCHEMA IF NOT EXISTS RAW;
CREATE SCHEMA IF NOT EXISTS PROCESSED;

-- Use existing warehouse
USE WAREHOUSE COMPUTE_WH;

-- =====================================================
-- STAGE SETUP
-- =====================================================

CREATE STAGE IF NOT EXISTS Documents 
	DIRECTORY = ( ENABLE = true ) 
	ENCRYPTION = ( TYPE = 'SNOWFLAKE_SSE' );


-- =====================================================
-- RAW TABLE - Store parsed PDF content
-- =====================================================

CREATE OR REPLACE TABLE RAW.PARSED_DOCUMENTS (
    document_id VARCHAR(50) PRIMARY KEY,
    file_name VARCHAR(255),
    report_year INTEGER,
    report_period VARCHAR(50), -- e.g., 'FY2024', 'H1 2024'
    parsed_content VARIANT,
    parsed_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- =====================================================
-- PROCESSED TABLE - Income Statement
-- =====================================================

CREATE OR REPLACE TABLE PROCESSED.INCOME_STATEMENT (
    statement_id VARCHAR(50) PRIMARY KEY,
    document_id VARCHAR(50),
    report_period VARCHAR(50),
    report_year INTEGER,
    
    -- Revenue Items (in USD Millions)
    total_revenue DECIMAL(15,2),
    lease_rental_income DECIMAL(15,2),
    net_gain_disposal_aircraft DECIMAL(15,2),
    other_income DECIMAL(15,2),
    
    -- Operating Expenses (in USD Millions)
    depreciation DECIMAL(15,2),
    staff_costs DECIMAL(15,2),
    other_operating_expenses DECIMAL(15,2),
    
    -- Operating Profit (in USD Millions)
    operating_profit DECIMAL(15,2),
    
    -- Finance Costs (in USD Millions)
    interest_expense DECIMAL(15,2),
    other_finance_costs DECIMAL(15,2),
    
    -- Profit Metrics (in USD Millions)
    profit_before_tax DECIMAL(15,2),
    income_tax_expense DECIMAL(15,2),
    profit_after_tax DECIMAL(15,2),
    profit_attributable_to_shareholders DECIMAL(15,2),
    
    -- Per Share Metrics (in USD)
    basic_earnings_per_share DECIMAL(10,4),
    diluted_earnings_per_share DECIMAL(10,4),
    
    -- Metadata
    currency VARCHAR(3) DEFAULT 'USD',
    unit VARCHAR(20) DEFAULT 'MILLION',
    processed_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
    
);

-- =====================================================
-- STEP 1: Parse PDF using AI_PARSE_DOCUMENT
-- =====================================================
USE SCHEMA RAW;
CREATE OR REPLACE PROCEDURE PARSE_FINANCIAL_REPORT(
    FILE_PATH VARCHAR,
    REPORT_YEAR INTEGER,
    REPORT_PERIOD VARCHAR
)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    doc_id VARCHAR;
    parsed_content VARIANT;
BEGIN
    -- Generate unique document ID
    doc_id := 'DOC_' || REPORT_YEAR || '_' || REPLACE(REPORT_PERIOD, ' ', '_');
    
    -- Use AI_PARSE_DOCUMENT to extract text from PDF with OCR
    -- Mode: LAYOUT preserves document structure including tables
    parsed_content := PARSE_JSON(
        AI_PARSE_DOCUMENT(
            '@FINANCIAL_REPORTS_STAGE',
            FILE_PATH,
            OBJECT_CONSTRUCT(
                'mode', 'LAYOUT',
                'page_split', TRUE
            )
        )
    );
    
    -- Store parsed content
    INSERT INTO RAW.PARSED_DOCUMENTS (
        document_id,
        file_name,
        report_year,
        report_period,
        parsed_content
    ) VALUES (
        doc_id,
        FILE_PATH,
        REPORT_YEAR,
        REPORT_PERIOD,
        parsed_content
    );
    
    RETURN 'Document parsed successfully. Document ID: ' || doc_id;
END;
$$;

-- =====================================================
-- STEP 2: Extract Income Statement using AI_EXTRACT
-- =====================================================

CREATE OR REPLACE PROCEDURE EXTRACT_INCOME_STATEMENT(DOCUMENT_ID VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    parsed_text VARCHAR;
    extracted_data VARIANT;
BEGIN
    -- Get the parsed document content
    SELECT parsed_content:content::VARCHAR INTO parsed_text
    FROM RAW.PARSED_DOCUMENTS
    WHERE document_id = DOCUMENT_ID;
    
    -- Use AI_EXTRACT to pull out specific income statement line items
    extracted_data := PARSE_JSON(
        AI_EXTRACT(
            parsed_text,
            OBJECT_CONSTRUCT(
                -- Revenue section
                'total_revenue', 'Extract the total revenue or total income amount in millions USD',
                'lease_rental_income', 'Extract the lease rental income or lease rentals amount in millions USD',
                'net_gain_disposal', 'Extract the net gain on disposal of aircraft amount in millions USD',
                'other_income', 'Extract other income or other revenue amount in millions USD',
                
                -- Operating expenses
                'depreciation', 'Extract the depreciation expense amount in millions USD',
                'staff_costs', 'Extract staff costs or employee costs amount in millions USD',
                'other_operating_expenses', 'Extract other operating expenses amount in millions USD',
                
                -- Operating profit
                'operating_profit', 'Extract the operating profit or profit from operations amount in millions USD',
                
                -- Finance costs
                'interest_expense', 'Extract interest expense or finance costs amount in millions USD',
                
                -- Profit metrics
                'profit_before_tax', 'Extract profit before tax or profit before income tax amount in millions USD',
                'income_tax_expense', 'Extract income tax expense or taxation amount in millions USD',
                'profit_after_tax', 'Extract profit after tax or profit for the period amount in millions USD',
                'profit_attributable_to_shareholders', 'Extract profit attributable to shareholders or profit attributable to owners amount in millions USD',
                
                -- Per share
                'basic_eps', 'Extract basic earnings per share in USD',
                'diluted_eps', 'Extract diluted earnings per share in USD'
            )
        )
    );
    
    -- Insert extracted data into income statement table
    INSERT INTO PROCESSED.INCOME_STATEMENT (
        statement_id,
        document_id,
        report_period,
        report_year,
        total_revenue,
        lease_rental_income,
        net_gain_disposal_aircraft,
        other_income,
        depreciation,
        staff_costs,
        other_operating_expenses,
        operating_profit,
        interest_expense,
        profit_before_tax,
        income_tax_expense,
        profit_after_tax,
        profit_attributable_to_shareholders,
        basic_earnings_per_share,
        diluted_earnings_per_share
    )
    SELECT 
        'IS_' || DOCUMENT_ID,
        DOCUMENT_ID,
        pd.report_period,
        pd.report_year,
        TRY_TO_NUMBER(extracted_data:total_revenue::VARCHAR),
        TRY_TO_NUMBER(extracted_data:lease_rental_income::VARCHAR),
        TRY_TO_NUMBER(extracted_data:net_gain_disposal::VARCHAR),
        TRY_TO_NUMBER(extracted_data:other_income::VARCHAR),
        TRY_TO_NUMBER(extracted_data:depreciation::VARCHAR),
        TRY_TO_NUMBER(extracted_data:staff_costs::VARCHAR),
        TRY_TO_NUMBER(extracted_data:other_operating_expenses::VARCHAR),
        TRY_TO_NUMBER(extracted_data:operating_profit::VARCHAR),
        TRY_TO_NUMBER(extracted_data:interest_expense::VARCHAR),
        TRY_TO_NUMBER(extracted_data:profit_before_tax::VARCHAR),
        TRY_TO_NUMBER(extracted_data:income_tax_expense::VARCHAR),
        TRY_TO_NUMBER(extracted_data:profit_after_tax::VARCHAR),
        TRY_TO_NUMBER(extracted_data:profit_attributable_to_shareholders::VARCHAR),
        TRY_TO_NUMBER(extracted_data:basic_eps::VARCHAR),
        TRY_TO_NUMBER(extracted_data:diluted_eps::VARCHAR)
    FROM RAW.PARSED_DOCUMENTS pd
    WHERE pd.document_id = DOCUMENT_ID;
    
    RETURN 'Income statement extracted successfully for document: ' || DOCUMENT_ID;
END;
$$;

-- =====================================================
-- STEP 3: Complete Workflow - Parse & Extract
-- =====================================================

CREATE OR REPLACE PROCEDURE PROCESS_INCOME_STATEMENT(
    FILE_PATH VARCHAR,
    REPORT_YEAR INTEGER,
    REPORT_PERIOD VARCHAR
)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    parse_result VARCHAR;
    doc_id VARCHAR;
    extract_result VARCHAR;
BEGIN
    -- Step 1: Parse PDF with OCR
    CALL PARSE_FINANCIAL_REPORT(FILE_PATH, REPORT_YEAR, REPORT_PERIOD) INTO parse_result;
    
    -- Extract document ID from result
    doc_id := 'DOC_' || REPORT_YEAR || '_' || REPLACE(REPORT_PERIOD, ' ', '_');
    
    -- Step 2: Extract income statement data
    CALL EXTRACT_INCOME_STATEMENT(doc_id) INTO extract_result;
    
    RETURN 'Processing complete. ' || parse_result || ' | ' || extract_result;
END;
$$;

-- =====================================================
-- DYNAMIC TABLE - Real-time Income Statement View
-- =====================================================

CREATE OR REPLACE DYNAMIC TABLE PROCESSED.INCOME_STATEMENT_TRENDS
    TARGET_LAG = '1 minute'
    WAREHOUSE = COMPUTE_WH
AS
SELECT 
    report_year,
    report_period,
    total_revenue,
    operating_profit,
    profit_after_tax,
    profit_attributable_to_shareholders,
    basic_earnings_per_share,
    
    -- Calculate margins
    ROUND((operating_profit / NULLIF(total_revenue, 0) * 100), 2) as operating_margin_pct,
    ROUND((profit_after_tax / NULLIF(total_revenue, 0) * 100), 2) as net_margin_pct,
    
    -- Year-over-year comparison
    LAG(total_revenue) OVER (ORDER BY report_year, report_period) as prior_period_revenue,
    LAG(profit_after_tax) OVER (ORDER BY report_year, report_period) as prior_period_profit,
    
    ROUND(
        ((total_revenue - LAG(total_revenue) OVER (ORDER BY report_year, report_period)) / 
        NULLIF(LAG(total_revenue) OVER (ORDER BY report_year, report_period), 0) * 100), 
        2
    ) as revenue_growth_pct,
    
    ROUND(
        ((profit_after_tax - LAG(profit_after_tax) OVER (ORDER BY report_year, report_period)) / 
        NULLIF(LAG(profit_after_tax) OVER (ORDER BY report_year, report_period), 0) * 100), 
        2
    ) as profit_growth_pct,
    
    processed_at
FROM PROCESSED.INCOME_STATEMENT
ORDER BY report_year DESC, report_period DESC;

-- =====================================================
-- STREAM - Track new income statements
-- =====================================================

CREATE OR REPLACE STREAM INCOME_STATEMENT_STREAM 
    ON TABLE PROCESSED.INCOME_STATEMENT;

-- =====================================================
-- TASK - Auto-refresh dynamic table
-- =====================================================

CREATE OR REPLACE TASK REFRESH_INCOME_STATEMENT_TRENDS
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = '5 MINUTE'
    WHEN SYSTEM$STREAM_HAS_DATA('INCOME_STATEMENT_STREAM')
AS
    ALTER DYNAMIC TABLE PROCESSED.INCOME_STATEMENT_TRENDS REFRESH;

-- Enable task
ALTER TASK REFRESH_INCOME_STATEMENT_TRENDS RESUME;

-- =====================================================
-- REPORTING VIEWS
-- =====================================================

-- Summary view for dashboards
CREATE OR REPLACE VIEW PROCESSED.INCOME_STATEMENT_SUMMARY AS
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
FROM PROCESSED.INCOME_STATEMENT_TRENDS
ORDER BY report_year DESC, report_period DESC;

-- Detailed view with all line items
CREATE OR REPLACE VIEW PROCESSED.INCOME_STATEMENT_DETAILED AS
SELECT 
    is1.report_year,
    is1.report_period,
    
    -- Revenue breakdown
    is1.total_revenue,
    is1.lease_rental_income,
    is1.net_gain_disposal_aircraft,
    is1.other_income,
    ROUND((is1.lease_rental_income / NULLIF(is1.total_revenue, 0) * 100), 1) as lease_income_pct,
    
    -- Expense breakdown
    is1.depreciation,
    is1.staff_costs,
    is1.other_operating_expenses,
    (is1.depreciation + is1.staff_costs + is1.other_operating_expenses) as total_operating_expenses,
    
    -- Profit metrics
    is1.operating_profit,
    is1.interest_expense,
    is1.profit_before_tax,
    is1.income_tax_expense,
    is1.profit_after_tax,
    is1.profit_attributable_to_shareholders,
    
    -- Per share
    is1.basic_earnings_per_share,
    is1.diluted_earnings_per_share,
    
    -- Margins
    ROUND((is1.operating_profit / NULLIF(is1.total_revenue, 0) * 100), 2) as operating_margin,
    ROUND((is1.profit_after_tax / NULLIF(is1.total_revenue, 0) * 100), 2) as net_margin,
    ROUND((is1.income_tax_expense / NULLIF(is1.profit_before_tax, 0) * 100), 2) as effective_tax_rate,
    
    is1.currency,
    is1.unit,
    is1.processed_at
FROM PROCESSED.INCOME_STATEMENT is1
ORDER BY is1.report_year DESC, is1.report_period DESC;
