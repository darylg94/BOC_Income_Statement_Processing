# Snowflake Cortex Functions Reference

## Functions Used in This Pipeline

### 1. AI_PARSE_DOCUMENT
**Documentation**: https://docs.snowflake.com/en/user-guide/snowflake-cortex/parse-document

#### Our Usage:
```sql
SELECT AI_PARSE_DOCUMENT(
    TO_FILE('@RAW.Documents', FILE_PATH),
    {'mode': 'LAYOUT'}
) INTO parsed_result;
```

#### What it does:
- Extracts text, data, and layout from PDF documents
- LAYOUT mode preserves structure (tables, headers, reading order)
- Returns JSON with structure:
  ```json
  {
    "metadata": {"pageCount": 19},
    "pages": [
      {"content": "Page 1 content in Markdown..."},
      {"content": "Page 2 content in Markdown..."}
    ]
  }
  ```

#### Key Parameters:
- **TO_FILE('@stage', 'filename')**: Specifies the file location
- **mode**: 'LAYOUT' (best for complex documents with tables) or 'OCR' (fast text extraction)
- **page_split**: Optional, splits multi-page docs into separate pages

#### Supported:
- **Formats**: PDF, DOCX, PPTX, HTML, TXT, images (JPEG, PNG, TIFF)
- **Languages**: English, Spanish, French, German, Italian, Portuguese, Russian, Chinese, Hindi, Turkish, Ukrainian, Romanian
- **Max Size**: 100 MB per file, 500 pages per PDF
- **Cost**: 970 tokens per page

---

### 2. AI_EXTRACT
**Documentation**: https://docs.snowflake.com/en/sql-reference/functions/ai_extract

#### Our Usage:
```sql
extracted_data := PARSE_JSON(
    AI_EXTRACT(
        parsed_text,
        OBJECT_CONSTRUCT(
            'total_revenue', 'Extract the total revenue or total income amount in millions USD',
            'lease_rental_income', 'Extract the lease rental income amount in millions USD',
            -- ... more fields
        )
    )
);
```

#### What it does:
- Extracts structured data from unstructured text using AI
- Takes natural language instructions for each field
- Returns JSON string with extracted values

#### Syntax:
```sql
AI_EXTRACT(
    <text_to_extract_from>,
    <extraction_instructions>
)
```

#### Parameters:
- **text**: VARCHAR - The source text (can be large)
- **instructions**: OBJECT - Key-value pairs where:
  - Key = field name you want
  - Value = natural language instruction describing what to extract

#### Returns:
- JSON string with extracted values
- Wrap in `PARSE_JSON()` to convert to VARIANT for easier querying

#### Example Instructions:
```sql
OBJECT_CONSTRUCT(
    'company_name', 'Extract the company name',
    'revenue', 'Extract total revenue in millions',
    'ceo', 'Extract the name of the CEO',
    'year', 'Extract the fiscal year'
)
```

---

## Our Complete Pipeline Flow

### Step 1: Parse Document (RAW.PARSE_FINANCIAL_REPORT)
```sql
-- Input: PDF file in stage
AI_PARSE_DOCUMENT(TO_FILE('@RAW.Documents', 'report.pdf'), {'mode': 'LAYOUT'})

-- Output: JSON with pages array
{
  "metadata": {"pageCount": 50},
  "pages": [
    {"content": "# BOC Aviation\n\n## Income Statement\n\n..."},
    ...
  ]
}
```

### Step 2: Extract Data (RAW.EXTRACT_INCOME_STATEMENT)
```sql
-- Input: Concatenated page content from all pages
SELECT LISTAGG(page_content.value:content::VARCHAR, '\n\n')
FROM ... LATERAL FLATTEN(input => parsed_content:pages)

-- Process with AI_EXTRACT
AI_EXTRACT(
    combined_text,
    OBJECT_CONSTRUCT(
        'total_revenue', 'Extract total revenue in millions USD',
        ...
    )
)

-- Output: JSON with extracted fields
{
  "total_revenue": "2135.7",
  "operating_profit": "845.2",
  ...
}
```

### Step 3: Load to Table
```sql
INSERT INTO PROCESSED.INCOME_STATEMENT (...)
SELECT 
    ...,
    TRY_TO_NUMBER(extracted_data:total_revenue::VARCHAR),
    TRY_TO_NUMBER(extracted_data:operating_profit::VARCHAR),
    ...
```

---

## Current Implementation Status

✅ **AI_PARSE_DOCUMENT**: Correctly implemented
- Using TO_FILE() for stage reference
- LAYOUT mode for complex financial documents
- Storing full JSON result

✅ **AI_EXTRACT**: Correctly implemented  
- Combining all pages with LISTAGG
- Using FLATTEN to process pages array
- Natural language instructions for each field
- PARSE_JSON wrapper for result

✅ **Data Flow**: Correctly structured
- Parse → Extract → Transform → Load
- Proper error handling with TRY_TO_NUMBER
- Foreign key relationships maintained

---

## Verification

### Check if AI_PARSE_DOCUMENT worked:
```sql
SELECT 
    document_id,
    file_name,
    parsed_content:metadata:pageCount as page_count,
    parsed_content:pages[0]:content::VARCHAR as first_page_preview
FROM RAW.PARSED_DOCUMENTS
ORDER BY parsed_at DESC
LIMIT 1;
```

### Check if AI_EXTRACT worked:
```sql
SELECT 
    report_period,
    total_revenue,
    operating_profit,
    profit_after_tax,
    basic_earnings_per_share
FROM PROCESSED.INCOME_STATEMENT
ORDER BY processed_at DESC
LIMIT 1;
```

---

## Troubleshooting

### AI_PARSE_DOCUMENT Errors

| Error | Solution |
|-------|----------|
| "File format not supported" | Ensure PDF is not encrypted; stage must use server-side encryption |
| "Maximum 500 pages exceeded" | Split document or process page ranges |
| "Maximum file size exceeded" | File must be < 100 MB |
| "Provided file cannot be found" | Check file path with `LIST @RAW.Documents` |

### AI_EXTRACT Errors

| Error | Solution |
|-------|----------|
| Returns NULL values | Check if text contains the requested information |
| Extraction inaccurate | Improve instruction prompts; be more specific |
| Timeout | Reduce text size or split into smaller chunks |

---

## Best Practices

1. **For AI_PARSE_DOCUMENT:**
   - Use LAYOUT mode for financial documents with tables
   - Enable page_split for very large documents
   - Store the full JSON result for debugging

2. **For AI_EXTRACT:**
   - Be specific in extraction instructions
   - Include units in the instruction (e.g., "in millions USD")
   - Use TRY_TO_NUMBER for numeric conversions
   - Test with one document before batch processing

3. **For Both:**
   - Monitor costs (970 tokens per page for parsing)
   - Use appropriate warehouse size (SMALL or MEDIUM)
   - Add error handling for production use

---

## References

- [AI_PARSE_DOCUMENT Documentation](https://docs.snowflake.com/en/user-guide/snowflake-cortex/parse-document)
- [AI_EXTRACT Documentation](https://docs.snowflake.com/en/sql-reference/functions/ai_extract)
- [Snowflake Cortex Overview](https://docs.snowflake.com/en/user-guide/snowflake-cortex/overview)

