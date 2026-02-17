-- =============================================================================
-- Nimble Agent Enrichment UDTF for Snowflake
-- =============================================================================
-- A batched table function that uses a LangChain agent with Nimble Search &
-- Extract tools to enrich rows. All rows in a partition are processed
-- sequentially by the agent in end_partition().
--
-- Prerequisites:
-- - Run 01_setup.sql first
-- - NIMBLE_DEVELOPER role or ACCOUNTADMIN
--
-- Usage:
--   SELECT
--       e.input:company_name::STRING AS company_name,
--       e.enriched:address::STRING AS address,
--       e.enriched:funding::STRING AS funding,
--       e.enriched:employees::STRING AS employees,
--       e.enriched:investors::STRING AS investors,
--       e.enriched:founders::STRING AS founders
--   FROM my_companies t,
--        TABLE(NIMBLE_INTEGRATION.ENRICHMENT.nimble_enrich(
--            TO_JSON(OBJECT_CONSTRUCT(
--                'company_name', t.company_name,
--                'website', t.website
--            )),
--            ARRAY_CONSTRUCT(
--                'address',
--                'funding',
--                'employees',
--                'investors',
--                'founders'
--            )
--        ) OVER (PARTITION BY 1)) e;
--
-- With custom prompt:
--   SELECT e.*
--   FROM my_table t,
--        TABLE(NIMBLE_INTEGRATION.ENRICHMENT.nimble_enrich(
--            TO_JSON(OBJECT_CONSTRUCT('company_name', t.name)),
--            ARRAY_CONSTRUCT('CEO name', 'Year founded', 'Revenue'),
--            'You are a company research agent. Find accurate, up-to-date info.'
--        ) OVER (PARTITION BY 1)) e;
-- =============================================================================

USE DATABASE NIMBLE_INTEGRATION;
USE SCHEMA ENRICHMENT;

-- =============================================================================
-- Agent-based UDTF
-- =============================================================================

CREATE OR REPLACE FUNCTION nimble_enrich(
    input_json VARCHAR,
    output_columns ARRAY,
    custom_prompt VARCHAR DEFAULT NULL
)
RETURNS TABLE (input VARIANT, enriched VARIANT)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
ARTIFACT_REPOSITORY = snowflake.snowpark.pypi_shared_repository
PACKAGES = ('langchain', 'langchain-anthropic', 'langchain-nimble', 'httpx')
HANDLER = 'NimbleEnrichHandler'
EXTERNAL_ACCESS_INTEGRATIONS = (nimble_api_access_integration)
SECRETS = ('nimble_key' = nimble_api_key, 'llm_key' = llm_api_key)
AS $$
import json
import os
import _snowflake


DEFAULT_SYSTEM_PROMPT = """
You are a company enrichment agent. Use this two-step approach:

**Step 1: Fast Search**
Use nimble_web_search with deep_search=false to get quick snippets and URLs.
Extract as much information as possible from the snippets.

**Step 2: Targeted Extraction (if needed)**
If information is still missing after search, use nimble_extract_content on
1-2 relevant URLs from the search results to get full page content.
Focus on official company websites, LinkedIn, or Crunchbase.

Return "Not found" for any field you cannot determine.
Be concise and factual. Do not hallucinate information.
"""


def _build_agent(nimble_api_key, llm_api_key, system_prompt):
    """Build a LangChain agent with Nimble tools."""
    from langchain_nimble import NimbleSearchTool, NimbleExtractTool
    from langchain_anthropic import ChatAnthropic
    from langchain.agents import create_agent

    os.environ["NIMBLE_API_KEY"] = nimble_api_key
    os.environ["ANTHROPIC_API_KEY"] = llm_api_key

    tools = [
        NimbleSearchTool(api_key=nimble_api_key),
        NimbleExtractTool(api_key=nimble_api_key),
    ]

    llm = ChatAnthropic(model="claude-sonnet-4-20250514", temperature=0, api_key=llm_api_key)

    return create_agent(
        model=llm,
        tools=tools,
        system_prompt=system_prompt,
    )


def _build_query(input_data, output_columns):
    """Build a natural-language query from input data and desired columns."""
    # Describe what we know
    context_parts = []
    for key, value in input_data.items():
        context_parts.append(f"{key}: {value}")
    context = ", ".join(context_parts)

    # Describe what we want
    columns = ", ".join(output_columns)

    return (
        f"Find the following information: [{columns}] "
        f"for the entity described by: {context}. "
        f"Return a JSON object with these exact keys: {json.dumps(output_columns)}. "
        f"Use \"Not found\" for any field you cannot determine."
    )


def _extract_text_from_content(content):
    """Extract plain text from AI message content (handles str and list-of-blocks)."""
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts = []
        for block in content:
            if isinstance(block, dict) and block.get("type") == "text":
                parts.append(block["text"])
            elif isinstance(block, str):
                parts.append(block)
        return "\n".join(parts)
    return str(content)


def _parse_agent_output(raw_output, output_columns):
    """Parse agent output into a dict keyed by output_columns."""
    text = raw_output if isinstance(raw_output, str) else str(raw_output)

    # Try to extract JSON from the output (supports nested braces)
    import re
    # Find the outermost JSON object by matching balanced braces
    depth = 0
    start = -1
    for i, ch in enumerate(text):
        if ch == '{':
            if depth == 0:
                start = i
            depth += 1
        elif ch == '}':
            depth -= 1
            if depth == 0 and start != -1:
                candidate = text[start:i+1]
                try:
                    parsed = json.loads(candidate)
                    # Normalise keys to match requested columns
                    result = {}
                    parsed_lower = {k.lower().replace(" ", "_"): v for k, v in parsed.items()}
                    for col in output_columns:
                        col_key = col.lower().replace(" ", "_")
                        if col_key in parsed_lower:
                            val = parsed_lower[col_key]
                            result[col] = json.dumps(val) if isinstance(val, (list, dict)) else str(val)
                        elif col in parsed:
                            val = parsed[col]
                            result[col] = json.dumps(val) if isinstance(val, (list, dict)) else str(val)
                        else:
                            result[col] = "Not found"
                    return result
                except json.JSONDecodeError:
                    start = -1
                    continue

    # Fallback: return the raw text so you can debug
    result = {col: "Not found" for col in output_columns}
    result["_raw"] = text
    return result


class NimbleEnrichHandler:
    def __init__(self):
        self.nimble_api_key = _snowflake.get_generic_secret_string("nimble_key")
        self.llm_api_key = _snowflake.get_generic_secret_string("llm_key")
        self.rows = []
        self.output_columns = []
        self.custom_prompt = None

    def process(self, input_json, output_columns, custom_prompt):
        """Collect rows into a batch. Called once per input row."""
        self.output_columns = list(output_columns) if output_columns else []
        if custom_prompt:
            self.custom_prompt = custom_prompt
        try:
            self.rows.append(json.loads(input_json) if input_json else {})
        except (json.JSONDecodeError, TypeError):
            self.rows.append({})

    def end_partition(self):
        """Process all collected rows using the agent. Called once per partition."""
        if not self.rows:
            return

        if not self.nimble_api_key:
            for row in self.rows:
                yield (row, {"error": "No Nimble API key configured"})
            return

        if not self.llm_api_key:
            for row in self.rows:
                yield (row, {"error": "No LLM API key configured"})
            return

        system_prompt = self.custom_prompt or DEFAULT_SYSTEM_PROMPT
        agent = _build_agent(
            self.nimble_api_key, self.llm_api_key, system_prompt
        )

        for row in self.rows:
            try:
                query = _build_query(row, self.output_columns)
                result = agent.invoke(
                    {"messages": [{"role": "user", "content": query}]}
                )
                # Extract the last AI message content
                output_text = ""
                messages = result.get("messages", [])
                for msg in reversed(messages):
                    if hasattr(msg, "type") and msg.type == "ai" and hasattr(msg, "content"):
                        output_text = _extract_text_from_content(msg.content)
                        break
                # If no AI message found, dump the full result for debugging
                if not output_text:
                    output_text = json.dumps(str(result))
                enriched = _parse_agent_output(output_text, self.output_columns)
                yield (row, enriched)
            except Exception as e:
                yield (row, {"error": str(e)})
$$;

-- =============================================================================
-- Grant permissions
-- =============================================================================

GRANT USAGE ON FUNCTION nimble_enrich(VARCHAR, ARRAY, VARCHAR) TO ROLE NIMBLE_USER;

-- =============================================================================
-- Verification
-- =============================================================================

SELECT 'nimble_enrich() UDTF created successfully' AS status;
