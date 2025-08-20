## ins_claims_audit
Using Snowflake Cortex Analyst, Cortex Search, Cortex AI SQL, and Streamlit, we are receiving claims data and allowing users to ask both predefined and free-form questions about the claims. We are also describing images and comparing them to claim descriptions to determine if the supporting evidence aligns with the description.


# Setup
In the setup.sql file we create the following objects:

* database: ins_co
* schema: loss_claims
* stage: loss evidence
*  tables:
  * Claims
  * Claim Lines
  * Financial Transactions
  * Authorization
  * Parsed_invoices
  * Parsed_guidelines
  * Parsed_claim_notes
 
