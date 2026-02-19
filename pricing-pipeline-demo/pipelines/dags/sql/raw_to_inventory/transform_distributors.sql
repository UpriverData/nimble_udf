-- =====================================================================
-- Transform: RAW.PARTNER_CONTRACTS -> INVENTORY.DISTRIBUTORS
-- Schedule: Daily @ 06:00 UTC
-- =====================================================================
-- Transforms raw contract data into the clean distributor dimension.
-- Only includes contracts with ACTIVE status.

MERGE INTO ENTERPRISE_DW.INVENTORY.DISTRIBUTORS AS tgt
USING (
    SELECT
        'DIST-' || LPAD(ROW_NUMBER() OVER (ORDER BY EFFECTIVE_DATE)::VARCHAR, 3, '0')
                                                           AS DISTRIBUTOR_ID,
        -- Clean up legal suffixes from partner names
        REGEXP_REPLACE(PARTNER_NAME, ',?\\s*(Inc\\.|Corp\\.|Corporation|Co\\.\\s*,?\\s*Inc\\.)$', '')
                                                           AS DISTRIBUTOR_NAME,
        PARTNER_DOMAIN                                     AS WEBSITE_DOMAIN,
        SPLIT_PART(PARTNER_REGION, ' - ', 1)               AS REGION,
        CHANNEL_TYPE                                        AS CHANNEL,
        TIER_LEVEL                                          AS PARTNERSHIP_TIER,
        TRY_TO_DATE(EFFECTIVE_DATE)                         AS CONTRACT_START,
        CASE
            WHEN CONTRACT_STATUS = 'ACTIVE' THEN 'ACTIVE'
            ELSE 'INACTIVE'
        END                                                AS STATUS
    FROM ENTERPRISE_DW.RAW.PARTNER_CONTRACTS
    WHERE CONTRACT_STATUS = 'ACTIVE'
) AS src
ON tgt.WEBSITE_DOMAIN = src.WEBSITE_DOMAIN
WHEN MATCHED THEN UPDATE SET
    tgt.DISTRIBUTOR_NAME = src.DISTRIBUTOR_NAME,
    tgt.REGION           = src.REGION,
    tgt.CHANNEL          = src.CHANNEL,
    tgt.PARTNERSHIP_TIER = src.PARTNERSHIP_TIER,
    tgt.CONTRACT_START   = src.CONTRACT_START,
    tgt.STATUS           = src.STATUS
WHEN NOT MATCHED THEN INSERT (
    DISTRIBUTOR_ID, DISTRIBUTOR_NAME, WEBSITE_DOMAIN, REGION,
    CHANNEL, PARTNERSHIP_TIER, CONTRACT_START, STATUS
) VALUES (
    src.DISTRIBUTOR_ID, src.DISTRIBUTOR_NAME, src.WEBSITE_DOMAIN, src.REGION,
    src.CHANNEL, src.PARTNERSHIP_TIER, src.CONTRACT_START, src.STATUS
);
