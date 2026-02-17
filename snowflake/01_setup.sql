-- =============================================================================
-- Nimble API External Access Setup for Snowflake
-- =============================================================================
-- This script sets up the network infrastructure required to call the Nimble
-- API and an LLM provider from Snowflake UDTFs.
--
-- Prerequisites:
-- - ACCOUNTADMIN role or equivalent permissions
-- - Nimble API key from https://app.nimbleway.com/account_settings/api_keys
-- - Anthropic API key from https://console.anthropic.com/
--
-- Configuration:
-- Replace 'YOUR_NIMBLE_API_KEY' and 'YOUR_ANTHROPIC_API_KEY' with your actual keys.
-- =============================================================================

-- Create database and schema for Nimble integration
CREATE DATABASE IF NOT EXISTS NIMBLE_INTEGRATION;
CREATE SCHEMA IF NOT EXISTS NIMBLE_INTEGRATION.ENRICHMENT;

USE DATABASE NIMBLE_INTEGRATION;
USE SCHEMA ENRICHMENT;

-- =============================================================================
-- Step 1: Create Network Rule
-- =============================================================================
-- Allows HTTPS egress traffic to the Nimble API and Anthropic API endpoints.
-- The Nimble langchain tools use nimble-retriever.webit.live by default.

CREATE OR REPLACE NETWORK RULE nimble_api_network_rule
    MODE = EGRESS
    TYPE = HOST_PORT
    VALUE_LIST = (
        'nimble-retriever.webit.live:443',
        'api.anthropic.com:443'
    )
    COMMENT = 'Network rule for Nimble API and Anthropic API access';

-- =============================================================================
-- Step 2: Create Secrets
-- =============================================================================
-- Securely stores the API keys.

CREATE OR REPLACE SECRET nimble_api_key
    TYPE = GENERIC_STRING
    SECRET_STRING = 'YOUR_NIMBLE_API_KEY'
    COMMENT = 'Nimble API key for search and extraction';

CREATE OR REPLACE SECRET llm_api_key
    TYPE = GENERIC_STRING
    SECRET_STRING = 'YOUR_ANTHROPIC_API_KEY'
    COMMENT = 'Anthropic API key for Claude LLM';

-- =============================================================================
-- Step 3: Create External Access Integration
-- =============================================================================
-- Combines the network rule and secrets to enable UDTFs to make external API calls.

CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION nimble_api_access_integration
    ALLOWED_NETWORK_RULES = (nimble_api_network_rule)
    ALLOWED_AUTHENTICATION_SECRETS = (nimble_api_key, llm_api_key)
    ENABLED = TRUE
    COMMENT = 'External access integration for Nimble enrichment agent';

-- =============================================================================
-- Step 4: Grant PyPI Repository Access
-- =============================================================================
-- Required for UDTFs to install Python packages from PyPI.

GRANT DATABASE ROLE SNOWFLAKE.PYPI_REPOSITORY_USER TO ROLE ACCOUNTADMIN;

-- =============================================================================
-- Step 5: Create Roles
-- =============================================================================

CREATE ROLE IF NOT EXISTS NIMBLE_DEVELOPER;
CREATE ROLE IF NOT EXISTS NIMBLE_USER;

-- Grant PyPI access to developer role
GRANT DATABASE ROLE SNOWFLAKE.PYPI_REPOSITORY_USER TO ROLE NIMBLE_DEVELOPER;

-- Grant permissions to NIMBLE_DEVELOPER
GRANT USAGE ON DATABASE NIMBLE_INTEGRATION TO ROLE NIMBLE_DEVELOPER;
GRANT USAGE ON SCHEMA NIMBLE_INTEGRATION.ENRICHMENT TO ROLE NIMBLE_DEVELOPER;
GRANT CREATE FUNCTION ON SCHEMA NIMBLE_INTEGRATION.ENRICHMENT TO ROLE NIMBLE_DEVELOPER;
GRANT USAGE ON INTEGRATION nimble_api_access_integration TO ROLE NIMBLE_DEVELOPER;
GRANT READ ON SECRET nimble_api_key TO ROLE NIMBLE_DEVELOPER;
GRANT READ ON SECRET llm_api_key TO ROLE NIMBLE_DEVELOPER;

-- Grant permissions to NIMBLE_USER
GRANT USAGE ON DATABASE NIMBLE_INTEGRATION TO ROLE NIMBLE_USER;
GRANT USAGE ON SCHEMA NIMBLE_INTEGRATION.ENRICHMENT TO ROLE NIMBLE_USER;

-- =============================================================================
-- Verification
-- =============================================================================

SHOW NETWORK RULES LIKE 'nimble_api_network_rule';
SHOW SECRETS LIKE 'nimble_api_key';
SHOW SECRETS LIKE 'llm_api_key';
SHOW EXTERNAL ACCESS INTEGRATIONS LIKE 'nimble_api_access_integration';
SHOW ROLES LIKE 'NIMBLE_%';

SELECT 'Setup complete! Run 02_create_udtf.sql next.' AS status;
