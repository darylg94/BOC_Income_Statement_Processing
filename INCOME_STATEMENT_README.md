# BOC Aviation Income Statement Pipeline

## Overview
A simplified workflow to extract Income Statement data from BOC Aviation's financial reports using Snowflake's Document AI for OCR and structured data extraction.

## Workflow

```
┌─────────────────┐
│   Upload PDF    │
│    to Stage     │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ AI_PARSE_DOCUMENT│  ← OCR extracts text with layout
│   (LAYOUT mode)  │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│   AI_EXTRACT    │  ← Extracts specific line items
│  (Income Items) │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ Income Statement│  ← Structured table
│      Table      │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ Dynamic Tables  │  ← Real-time analytics
│   & Views       │
└─────────────────┘
```

## Setup

### 1. Create Infrastructure
```sql
@income_statement_pipeline.sql
```

### 2. Upload Reports
```sql
PUT file:///path/to/reports/*.pdf @FINANCIAL_REPORTS_STAGE;
```

### 3. Process Report
```sql
CALL PROCESS_INCOME_STATEMENT('report.pdf', 2024, 'FY2024');
```

## Data Extracted

### Income Statement Line Items
- **Revenue**
  - Total Revenue
  - Lease Rental Income
  - Net Gain on Disposal of Aircraft
  - Other Income

- **Operating Expenses**
  - Depreciation
  - Staff Costs
  - Other Operating Expenses

- **Profit Metrics**
  - Operating Profit
  - Interest Expense
  - Profit Before Tax
  - Income Tax Expense
  - Profit After Tax
  - Profit Attributable to Shareholders

- **Per Share**
  - Basic Earnings Per Share
  - Diluted Earnings Per Share

## Tables & Views

### Raw Layer
- `RAW.PARSED_DOCUMENTS` - Stores OCR'd PDF content

### Processed Layer
- `PROCESSED.INCOME_STATEMENT` - Structured income statement data
- `PROCESSED.INCOME_STATEMENT_TRENDS` - Dynamic table with YoY analysis
- `PROCESSED.INCOME_STATEMENT_SUMMARY` - Dashboard view
- `PROCESSED.INCOME_STATEMENT_DETAILED` - Full detail view

## Key Features

### 1. Document AI OCR
Uses `AI_PARSE_DOCUMENT` with LAYOUT mode to:
- Extract text from scanned PDFs
- Preserve table structures
- Handle multi-page documents

### 2. Intelligent Extraction
Uses `AI_EXTRACT` to:
- Identify specific financial line items
- Extract numerical values
- Handle various formats and labels

### 3. Automated Analytics
- **Dynamic Tables**: Auto-refresh every 1 minute
- **YoY Comparisons**: Automatic growth calculations
- **Margin Analysis**: Calculated operating and net margins

### 4. Real-time Updates
- **Streams**: Track new income statements
- **Tasks**: Auto-refresh analytics
- **Views**: Always up-to-date reporting

## Example Queries

### View Latest Results
```sql
SELECT * FROM PROCESSED.INCOME_STATEMENT_SUMMARY;
```

### Year-over-Year Growth
```sql
SELECT 
    report_period,
    total_revenue,
    revenue_growth_pct,
    profit_after_tax,
    profit_growth_pct
FROM PROCESSED.INCOME_STATEMENT_TRENDS
WHERE prior_period_revenue IS NOT NULL;
```

### Profitability Analysis
```sql
SELECT 
    report_period,
    operating_margin_pct,
    net_margin_pct,
    basic_earnings_per_share
FROM PROCESSED.INCOME_STATEMENT_SUMMARY;
```

### Revenue Composition
```sql
SELECT 
    report_period,
    lease_rental_income,
    net_gain_disposal_aircraft,
    other_income,
    ROUND((lease_rental_income / total_revenue * 100), 1) as lease_pct
FROM PROCESSED.INCOME_STATEMENT;
```

## Processing Flow

### Step 1: Parse PDF (OCR)
```sql
CALL PARSE_FINANCIAL_REPORT(
    'report.pdf',    -- File name in stage
    2024,            -- Report year
    'FY2024'         -- Report period
);
```

### Step 2: Extract Income Statement
```sql
CALL EXTRACT_INCOME_STATEMENT('DOC_2024_FY2024');
```

### Step 3: Complete Workflow
```sql
-- Or use the combined procedure
CALL PROCESS_INCOME_STATEMENT('report.pdf', 2024, 'FY2024');
```

## Monitoring

### Check Processing Status
```sql
SELECT 
    document_id,
    file_name,
    report_period,
    parsed_at
FROM RAW.PARSED_DOCUMENTS;
```

### View Extracted Data
```sql
SELECT * FROM PROCESSED.INCOME_STATEMENT;
```

### Check Real-time Updates
```sql
-- Stream status
SELECT SYSTEM$STREAM_HAS_DATA('INCOME_STATEMENT_STREAM');

-- Dynamic table refresh
SELECT * FROM TABLE(INFORMATION_SCHEMA.DYNAMIC_TABLES())
WHERE TABLE_NAME = 'INCOME_STATEMENT_TRENDS';
```

## Advantages

1. **Simple**: 3 stored procedures, clear workflow
2. **Automated**: OCR + extraction in one process
3. **Accurate**: Document AI handles complex PDF layouts
4. **Real-time**: Dynamic tables auto-refresh
5. **Scalable**: Process multiple reports quickly

## Troubleshooting

### PDF Not Parsing
- Verify file uploaded: `LIST @FINANCIAL_REPORTS_STAGE;`
- Check file format is readable
- Ensure sufficient warehouse size

### Missing Data
- Review extracted values: `SELECT * FROM PROCESSED.INCOME_STATEMENT;`
- Check if line item labels match extraction queries
- Adjust AI_EXTRACT prompts if needed

### Dynamic Table Not Updating
- Check stream: `SELECT * FROM INCOME_STATEMENT_STREAM;`
- Verify task is running: `SHOW TASKS;`
- Check target lag: May need to wait for refresh

## Files Included

1. `income_statement_pipeline.sql` - Pipeline setup
2. `run_income_statement_pipeline.sql` - Execution script
3. `INCOME_STATEMENT_README.md` - This file

## Quick Start

```sql
-- 1. Setup
@income_statement_pipeline.sql

-- 2. Process report
@run_income_statement_pipeline.sql

-- 3. View results
SELECT * FROM PROCESSED.INCOME_STATEMENT_SUMMARY;
```

## Next Steps

To extend this pipeline:
- Add balance sheet extraction
- Include cash flow statement
- Add data quality checks
- Create additional analytics views
- Build visualization dashboards
