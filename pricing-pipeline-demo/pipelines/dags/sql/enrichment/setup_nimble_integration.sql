-- =============================================================================
-- Nimble API External Access Setup for Snowflake
-- =============================================================================
-- Idempotent setup of network rules, secrets, and external access integration
-- required for the nimble_enrich() UDTF to call external APIs.
-- =============================================================================

CREATE DATABASE IF NOT EXISTS NIMBLE_INTEGRATION;
CREATE SCHEMA IF NOT EXISTS NIMBLE_INTEGRATION.ENRICHMENT;

USE DATABASE NIMBLE_INTEGRATION;
USE SCHEMA ENRICHMENT;

-- Network rule: allow HTTPS egress to Nimble API and Anthropic API
CREATE OR REPLACE NETWORK RULE nimble_api_network_rule
    MODE = EGRESS
    TYPE = HOST_PORT
    VALUE_LIST = (
        'nimble-retriever.webit.live:443',
        'api.anthropic.com:443'
    )
    COMMENT = 'Network rule for Nimble API and Anthropic API access';

-- Secrets for API keys
CREATE OR REPLACE SECRET nimble_api_key
    TYPE = GENERIC_STRING
    SECRET_STRING = 'YOUR_NIMBLE_API_KEY'
    COMMENT = 'Nimble API key for search and extraction';

CREATE OR REPLACE SECRET llm_api_key
    TYPE = GENERIC_STRING
    SECRET_STRING = 'YOUR_ANTHROPIC_API_KEY'
    COMMENT = 'Anthropic API key for Claude LLM';

-- External access integration combining network rule + secrets
CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION nimble_api_access_integration
    ALLOWED_NETWORK_RULES = (nimble_api_network_rule)
    ALLOWED_AUTHENTICATION_SECRETS = (nimble_api_key, llm_api_key)
    ENABLED = TRUE
    COMMENT = 'External access integration for Nimble enrichment agent';

-- PyPI access for Python packages in UDTFs
GRANT DATABASE ROLE SNOWFLAKE.PYPI_REPOSITORY_USER TO ROLE ACCOUNTADMIN;

-- Roles
CREATE ROLE IF NOT EXISTS NIMBLE_DEVELOPER;
CREATE ROLE IF NOT EXISTS NIMBLE_USER;

GRANT DATABASE ROLE SNOWFLAKE.PYPI_REPOSITORY_USER TO ROLE NIMBLE_DEVELOPER;
GRANT USAGE ON DATABASE NIMBLE_INTEGRATION TO ROLE NIMBLE_DEVELOPER;
GRANT USAGE ON SCHEMA NIMBLE_INTEGRATION.ENRICHMENT TO ROLE NIMBLE_DEVELOPER;
GRANT CREATE FUNCTION ON SCHEMA NIMBLE_INTEGRATION.ENRICHMENT TO ROLE NIMBLE_DEVELOPER;
GRANT USAGE ON INTEGRATION nimble_api_access_integration TO ROLE NIMBLE_DEVELOPER;
GRANT READ ON SECRET nimble_api_key TO ROLE NIMBLE_DEVELOPER;
GRANT READ ON SECRET llm_api_key TO ROLE NIMBLE_DEVELOPER;
GRANT USAGE ON DATABASE NIMBLE_INTEGRATION TO ROLE NIMBLE_USER;
GRANT USAGE ON SCHEMA NIMBLE_INTEGRATION.ENRICHMENT TO ROLE NIMBLE_USER;

SELECT 'Nimble integration setup complete.' AS status;