Create database if not exists ins_co;

create schema if not exists ins_co.loss_claims;

CREATE TABLE IF NOT EXISTS CLAIMS (
    CLAIM_NO VARCHAR,
    LINE_OF_BUSINESS VARCHAR,
    CLAIM_STATUS VARCHAR,
    CAUSE_OF_LOSS VARCHAR,
    CREATED_DATE DATE,
    LOSS_DATE DATE,
    REPORTED_DATE DATE,
    CLAIMANT_ID VARCHAR,
    PERFORMER VARCHAR,
    POLICY_NO VARCHAR,
    FNOL_COMPLETION_DATE DATE,
    LOSS_DESCRIPTION VARCHAR,
    LOSS_STATE VARCHAR,
    LOSS_ZIP_CODE VARCHAR
);


CREATE TABLE IF NOT EXISTS CLAIM_LINES (
    CLAIM_NO VARCHAR,
    LINE_NO INT,
    LOSS_DESCRIPTION VARCHAR,
    CLAIM_STATUS VARCHAR,
    CREATED_DATE DATE,
    REPORTED_DATE DATE,
    CLAIMANT_ID VARCHAR,
    PERFORMER_ID VARCHAR
);

CREATE TABLE IF NOT EXISTS FINANCIAL_TRANSACTIONS (
    FXID VARCHAR,
    LINE_NO INT,
    FINANCIAL_TYPE VARCHAR,
    CURRENCY VARCHAR,
    FIN_TX_AMT DECIMAL(18, 2),
    FIN_TX_POST_DT DATE
);

select * from claims;

INSERT INTO CLAIMS (
    CLAIM_NO, LINE_OF_BUSINESS, CLAIM_STATUS, CAUSE_OF_LOSS,
    CREATED_DATE, LOSS_DATE, REPORTED_DATE, CLAIMANT_ID, PERFORMER,
    POLICY_NO, FNOL_COMPLETION_DATE, LOSS_DESCRIPTION, LOSS_STATE, LOSS_ZIP_CODE
) VALUES
('1899', 'Property', 'Open', 'Hurricane', '2025-01-06', '2025-01-06', '2025-01-06', '19', '18',
 '888', '01/06/2025', 'Damaged dwelling and fence after the tree fell', 'NJ', '8820');


INSERT INTO CLAIM_LINES (
    CLAIM_NO, LINE_NO, LOSS_DESCRIPTION, CLAIM_STATUS,
    CREATED_DATE, REPORTED_DATE, CLAIMANT_ID, PERFORMER_ID
) VALUES
('1899', 16, 'Damaged Dwelling', 'Open', '2025-01-06', '2025-01-06', '19', '171'),
('1899', 17, 'Damaged Fence', 'Open', '2025-01-06', '2025-01-06', '19', '181'),
('1899', 18, 'Damaged Lawn', 'Open', '2025-01-06', '2025-01-06', '19', '191');


select * from financial_transactions;

INSERT INTO FINANCIAL_TRANSACTIONS (
    FXID, LINE_NO, FINANCIAL_TYPE, CURRENCY, FIN_TX_AMT, FIN_TX_POST_DT
) VALUES
('21', 16, 'RSV', 'USD', 4000.00, '2025-02-15'), 
('22', 16, 'PAY', 'USD', 4000.00, '2025-06-15'), -- Mapped to Line 18 (Damaged Dwelling)
('23', 17, 'RSV', 'USD', 3000.00, '2025-03-06'), -- Mapped to Line 19 (Damaged Fence)
('24', 17, 'PAY', 'USD', 3500.00, '2025-05-05'), -- Mapped to Line 19 (Damaged Fence)
('25', 18, 'RSV', 'USD', 2000.00, '2025-02-15'), -- Mapped to Line 20 (Damaged Lawn)
('26', 18, 'PAY', 'USD', 2000.00, '2025-04-05'); -- Mapped to Line 20 (Damaged Lawn);


CREATE TABLE IF NOT EXISTS AUTHORIZATION (
    PERFORMER_ID VARCHAR(50) PRIMARY KEY,
    FROM_AMT DECIMAL(18, 2),
    TO_AMT DECIMAL(18, 2),
    CURRENCY VARCHAR(10)
);

select * from authorization;


INSERT INTO AUTHORIZATION (PERFORMER_ID, FROM_AMT, TO_AMT, CURRENCY) VALUES
('171', 0.00, 5000.00, 'USD'),
('181', 0.00, 3000.00, 'USD'),
('191', 0.00, 2500.00, 'USD');


CREATE TABLE IF NOT EXISTS INVOICES (
    INV_ID VARCHAR,
    INV_LINE_NBR VARCHAR,
    LINE_NO VARCHAR,
    DESCRIPTION VARCHAR,
    CURRENCY VARCHAR(10),
    INVOICE_AMOUNT DECIMAL(18, 2),
    INVOICE_DATE DATE,
    VENDOR VARCHAR
);

INSERT INTO INVOICES (INV_ID, INV_LINE_NBR, LINE_NO, DESCRIPTION, CURRENCY, INVOICE_AMOUNT, INVOICE_DATE, VENDOR) VALUES
('5', 1, 16, 'Wooden Logs', 'USD', 2500.00, '2025-05-15', 'ABC'),
('5', 2, 16, 'Hardware', 'USD', 1000.00, '2025-05-15', 'ABC'),
('5', 3, 16, 'Labor', 'USD', 500.00, '2025-05-15', 'ABC'),
('6', 1, 17, 'Fence', 'USD', 3000.00, '2025-04-20', 'LMN'),
('6', 2, 17, 'Labor', 'USD', 500.00, '2025-04-20', 'LMN'),
('7', 1, 18, 'Lawn', 'USD', 1200.00, '2025-03-18', 'XYZ'),
('7', 2, 18, 'Equipment Rental', 'USD', 200.00, '2025-03-18', 'XYZ'),
('7', 3, 18, 'Labor', 'USD', 600.00, '2025-03-18', 'XYZ');

select * from invoices;

CREATE STAGE if not exists loss_evidence 
	DIRECTORY = ( ENABLE = true ) 
	ENCRYPTION = ( TYPE = 'SNOWFLAKE_SSE' );

-- upload claim photo evidence to loss_evidence stage
-- upload guidelines
-- upload claim notes
-- upload invoice



CREATE TABLE IF NOT EXISTS 
PARSED_CLAIM_NOTES (
    FILENAME VARCHAR(255),
    EXTRACTED_CONTENT VARCHAR(16777216),
    PARSE_DATE TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP,
    CLAIM_NO VARCHAR
);


INSERT INTO PARSED_CLAIM_NOTES (FILENAME, EXTRACTED_CONTENT, CLAIM_NO)
SELECT
    t1.RELATIVE_PATH AS FILENAME,
    t1.EXTRACTED_CONTENT,
    flattened.value:answer::VARCHAR AS CLAIM_NO
FROM
    (
        SELECT
            RELATIVE_PATH,
            TO_VARCHAR(
                SNOWFLAKE.CORTEX.PARSE_DOCUMENT(
                    '@INS_CO.loss_claims.loss_evidence',
                    RELATIVE_PATH,
                    {'mode': 'OCR'}
                ):content
            ) AS EXTRACTED_CONTENT
        FROM
            DIRECTORY('@INS_CO.loss_claims.loss_evidence')
        WHERE
            RELATIVE_PATH LIKE '%Claim_Note%'
    ) AS t1,
    LATERAL FLATTEN(
        input => SNOWFLAKE.CORTEX.EXTRACT_ANSWER(t1.EXTRACTED_CONTENT, 'What is the claim number?')
    ) AS flattened
WHERE
    flattened.value:score::NUMBER >= 0.5;
    
select * from parsed_claim_notes;

CREATE TABLE IF NOT EXISTS PARSED_GUIDELINES (
    FILENAME VARCHAR(255),
    EXTRACTED_CONTENT VARCHAR(16777216), 
    PARSE_DATE TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO PARSED_GUIDELINES (FILENAME, EXTRACTED_CONTENT)
SELECT
    t1.RELATIVE_PATH AS FILENAME,
    TO_VARCHAR(
        SNOWFLAKE.CORTEX.PARSE_DOCUMENT(
            '@INS_CO.loss_claims.loss_evidence',
            t1.RELATIVE_PATH,
            {'mode': 'OCR'}
        ):content
    ) AS EXTRACTED_CONTENT
FROM
    DIRECTORY('@INS_CO.loss_claims.loss_evidence') AS t1
WHERE
    t1.RELATIVE_PATH LIKE '%Guideline%';


select * from parsed_guidelines;

CREATE TABLE IF NOT EXISTS PARSED_INVOICES (
    FILENAME VARCHAR(255),
    EXTRACTED_CONTENT VARCHAR(16777216), 
    PARSE_DATE TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP,
    CLAIM_NO VARCHAR
);


INSERT INTO PARSED_INVOICES (FILENAME, EXTRACTED_CONTENT, CLAIM_NO)
SELECT
    t1.RELATIVE_PATH,
    t1.EXTRACTED_CONTENT,
    -- Extract the answer from the flattened JSON object
    flattened.value:answer::VARCHAR AS CLAIM_NO
FROM
    (
        SELECT
            RELATIVE_PATH,
            TO_VARCHAR(
                SNOWFLAKE.CORTEX.PARSE_DOCUMENT(
                    '@INS_CO.loss_claims.loss_evidence',
                    RELATIVE_PATH,
                    {'mode': 'OCR'}
                ):content
            ) AS EXTRACTED_CONTENT
        FROM
            DIRECTORY('@INS_CO.loss_claims.loss_evidence')
        WHERE
            RELATIVE_PATH LIKE '%invoice%'
    ) AS t1,
    LATERAL FLATTEN(
        input => SNOWFLAKE.CORTEX.EXTRACT_ANSWER(t1.EXTRACTED_CONTENT, 'What is the claim no?')
    ) AS flattened
WHERE
    flattened.value:score::NUMBER >= 0.5;

select * from parsed_invoices;


----- chunk data in claim notes and guidelines -----

-- CREATE OR REPLACE TABLE NOTES_CHUNK_TABLE AS
-- SELECT
--     FILENAME,
--     BUILD_SCOPED_FILE_URL('@INS_CO.loss_claims.loss_evidence', FILENAME) AS file_url,
--     CONCAT(FILENAME, ': ', c.value::TEXT) AS chunk,
--     'English' AS language
-- FROM
--     PARSED_CLAIM_NOTES,
--     LATERAL FLATTEN(SNOWFLAKE.CORTEX.SPLIT_TEXT_RECURSIVE_CHARACTER(
--         EXTRACTED_CONTENT,
--         'markdown',
--         200, -- chunks of 2000 characters
--         30 -- 300 character overlap
--     )) c;

CREATE OR REPLACE TABLE NOTES_CHUNK_TABLE AS
SELECT
    FILENAME,
    CLAIM_NO,  -- Add this line to include the claim number
    BUILD_SCOPED_FILE_URL('@INS_CO.loss_claims.loss_evidence', FILENAME) AS file_url,
    CONCAT(FILENAME, ': ', c.value::TEXT) AS chunk,
    'English' AS language
FROM
    PARSED_CLAIM_NOTES,
    LATERAL FLATTEN(SNOWFLAKE.CORTEX.SPLIT_TEXT_RECURSIVE_CHARACTER(
        EXTRACTED_CONTENT,
        'markdown',
        200, -- chunks of 200 characters
        30 -- 30 character overlap
    )) c;

select * from NOTES_CHUNK_TABLE;


CREATE OR REPLACE TABLE GUIDELINES_CHUNK_TABLE AS
SELECT
    FILENAME,
    BUILD_SCOPED_FILE_URL('@INS_CO.loss_claims.loss_evidence', FILENAME) AS file_url,
    CONCAT(FILENAME, ': ', c.value::TEXT) AS chunk,
    'English' AS language
FROM
    PARSED_GUIDELINES,
    LATERAL FLATTEN(SNOWFLAKE.CORTEX.SPLIT_TEXT_RECURSIVE_CHARACTER(
        EXTRACTED_CONTENT,
        'markdown',
        200, -- chunks of 2000 characters
        30 -- 300 character overlap
    )) c;


---- create cortex search services -----

CREATE 
-- OR REPLACE
CORTEX SEARCH SERVICE 
-- IF NOT EXISTS 
ins_co_claim_notes
  ON chunk
  -- NOTE_CONTENT
  -- ATTRIBUTES claim_no, note_date, note_id
  WAREHOUSE = compute_wh
  TARGET_LAG = '1 hour'
  EMBEDDING_MODEL = 'snowflake-arctic-embed-l-v2.0'
AS (
  SELECT
    chunk
     -- note_content, claim_no, note_date, note_id
  FROM NOTES_CHUNK_TABLE
);

CREATE 
-- OR REPLACE
CORTEX SEARCH SERVICE 
-- IF NOT EXISTS 
ins_co_guidelines
  ON chunk
  -- ATTRIBUTES claim_no, note_date, note_id
  WAREHOUSE = compute_wh
  TARGET_LAG = '1 hour'
  EMBEDDING_MODEL = 'snowflake-arctic-embed-l-v2.0'
AS (
  SELECT
     chunk
  FROM GUIDELINES_CHUNK_TABLE
);


--- test out the cortex search service

SELECT
  SNOWFLAKE.CORTEX.SEARCH_PREVIEW (
      'INS_CO_CLAIM_NOTES',
      '{
          "query": "reserve",
          "columns": ["chunk"],
          "limit": 2
      }'
  );


CREATE STAGE MODELS 
	DIRECTORY = ( ENABLE = true ) 
	ENCRYPTION = ( TYPE = 'SNOWFLAKE_SSE' );

--CREATE CORTEX ANALYST YAML FILE
--RUN STREAMLIT

select * from invoices;
