-- =====================================================================
-- Transform: RAW.SALESFORCE_ACCOUNTS -> CRM.CUSTOMERS
-- Schedule: Every 6 hours (after Fivetran Salesforce sync)
-- =====================================================================
-- Transforms raw Salesforce account/contact data into the clean
-- customer dimension. Filters out deleted and non-customer records.

MERGE INTO ENTERPRISE_DW.CRM.CUSTOMERS AS tgt
USING (
    SELECT
        -- Map Salesforce account ID to internal customer ID
        'CUST-' || REPLACE(SF_ACCOUNT_ID, '001Dn00000A', '200')
                                                         AS CUSTOMER_ID,
        FIRST_NAME,
        LAST_NAME,
        EMAIL_ADDRESS                                    AS EMAIL,
        -- Map Salesforce account type to customer segment
        CASE
            WHEN ACCOUNT_TYPE = 'Customer - Channel'  THEN 'VIP'
            WHEN TRY_CAST(ANNUAL_REVENUE AS NUMBER) >= 70000 THEN 'Premium'
            ELSE 'Standard'
        END                                              AS SEGMENT,
        -- Estimate lifetime value from annual revenue
        TRY_CAST(ANNUAL_REVENUE AS DECIMAL(12,2)) * 0.05
                                                         AS LIFETIME_VALUE,
        TRY_TO_DATE(LEFT(CREATED_DATE, 10))              AS SIGNUP_DATE,
        -- Map billing state to sales region
        CASE
            WHEN BILLING_STATE IN ('New York','Massachusetts','Connecticut','New Jersey')
                THEN 'Northeast'
            WHEN BILLING_STATE IN ('Georgia','Florida','Virginia','North Carolina')
                THEN 'Southeast'
            WHEN BILLING_STATE IN ('Illinois','Ohio','Michigan','Indiana')
                THEN 'Midwest'
            WHEN BILLING_STATE IN ('California','Oregon','Washington')
                THEN 'West'
            WHEN BILLING_STATE IN ('Arizona','Texas','New Mexico','Nevada')
                THEN 'Southwest'
            ELSE 'Other'
        END                                              AS REGION
    FROM ENTERPRISE_DW.RAW.SALESFORCE_ACCOUNTS
    WHERE IS_DELETED = FALSE
      AND ACCOUNT_TYPE ILIKE 'Customer%'
) AS src
ON tgt.CUSTOMER_ID = src.CUSTOMER_ID
WHEN MATCHED THEN UPDATE SET
    tgt.FIRST_NAME      = src.FIRST_NAME,
    tgt.LAST_NAME       = src.LAST_NAME,
    tgt.EMAIL           = src.EMAIL,
    tgt.SEGMENT         = src.SEGMENT,
    tgt.LIFETIME_VALUE  = src.LIFETIME_VALUE,
    tgt.SIGNUP_DATE     = src.SIGNUP_DATE,
    tgt.REGION          = src.REGION
WHEN NOT MATCHED THEN INSERT (
    CUSTOMER_ID, FIRST_NAME, LAST_NAME, EMAIL,
    SEGMENT, LIFETIME_VALUE, SIGNUP_DATE, REGION
) VALUES (
    src.CUSTOMER_ID, src.FIRST_NAME, src.LAST_NAME, src.EMAIL,
    src.SEGMENT, src.LIFETIME_VALUE, src.SIGNUP_DATE, src.REGION
);
