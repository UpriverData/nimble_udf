"""
Nimble Enrichment UDF - Setup & Registration (Unity Catalog)

Creates a Unity Catalog governed Python UDF that uses a LangChain agent
with Nimble Search & Extract tools to enrich any table with live web data.

Three functions are created:
1. _nimble_enrich_internal — Python UDF with credential + provider parameters
2. nimble_enrich           — SQL wrapper using ChatDatabricks (Model Serving)
3. nimble_enrich_anthropic — SQL wrapper using ChatAnthropic (Anthropic API)

Prerequisites:
- Databricks workspace with Unity Catalog
- Serverless or Pro SQL warehouse (or DBR 16.3+ cluster)
- Nimble API key stored in a Databricks secret scope
- Either: Databricks Model Serving endpoint with Claude, or Anthropic API key
"""

# --- Step 1: Store your API keys in Databricks Secrets ---
#
# Run this once from a terminal or the Databricks CLI:
#
#   databricks secrets create-scope nimble
#   databricks secrets put-secret nimble nimble_api_key
#   databricks secrets put-secret nimble databricks_token     # for ChatDatabricks
#   databricks secrets put-secret nimble anthropic_api_key    # for ChatAnthropic

# --- Step 2: Set catalog and schema ---

CATALOG = "main"
SCHEMA = "default"

spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA {SCHEMA}")
print(f"UDF will be registered as: {CATALOG}.{SCHEMA}.nimble_enrich")

# --- Step 3a: Create the internal Python UDF ---
# Note: Python UDFs in Databricks do not support DEFAULT parameter values,
# so all params are required. The SQL wrappers handle defaults.

sql = f"CREATE OR REPLACE FUNCTION {CATALOG}.{SCHEMA}._nimble_enrich_internal" + """(
    input_json       STRING,
    output_columns   STRING,
    nimble_api_key   STRING,
    llm_provider     STRING,
    llm_api_key      STRING,
    databricks_host  STRING,
    custom_prompt    STRING
)
RETURNS STRING
LANGUAGE PYTHON
NOT DETERMINISTIC
COMMENT 'Internal: enriches a row with live web data using a LangChain agent with Nimble tools. Use nimble_enrich() or nimble_enrich_anthropic() instead.'
ENVIRONMENT (
    dependencies = '["langchain", "langchain-community", "databricks-langchain", "langchain-anthropic", "langchain-nimble"]',
    environment_version = 'None'
)
AS $$
import json
import os

os.environ["NIMBLE_API_KEY"] = nimble_api_key

from langchain_nimble import NimbleSearchTool, NimbleExtractTool
from langchain.agents import create_agent

MAX_TOOL_OUTPUT_CHARS = 80000  # ~20k tokens — keeps total context well under 200k

def _make_truncated_tool(tool, max_chars=MAX_TOOL_OUTPUT_CHARS):
    orig_run = tool._run
    def truncated_run(*args, **kwargs):
        result = orig_run(*args, **kwargs)
        if isinstance(result, str) and len(result) > max_chars:
            return result[:max_chars] + "\\n\\n[OUTPUT TRUNCATED]"
        return result
    tool._run = truncated_run
    return tool

# Configure LLM based on provider
if llm_provider == "anthropic":
    os.environ["ANTHROPIC_API_KEY"] = llm_api_key
    from langchain_anthropic import ChatAnthropic
    llm = ChatAnthropic(model="claude-sonnet-4-20250514", api_key=llm_api_key)
else:
    os.environ["DATABRICKS_HOST"] = databricks_host
    os.environ["DATABRICKS_TOKEN"] = llm_api_key
    from databricks_langchain import ChatDatabricks
    llm = ChatDatabricks(endpoint="databricks-claude-sonnet-4-5")

# Parse inputs
try:
    input_data = json.loads(input_json) if input_json else {}
except (json.JSONDecodeError, TypeError):
    input_data = {}

try:
    output_cols = json.loads(output_columns) if output_columns else []
except (json.JSONDecodeError, TypeError):
    output_cols = []

if not output_cols:
    return json.dumps({"error": "No output columns specified"})

default_prompt = (
    "You are a data enrichment agent. Use this two-step approach:\\n\\n"
    "**Step 1: Fast Search**\\n"
    "Use nimble_web_search with deep_search=false to get quick snippets and URLs.\\n"
    "Extract as much information as possible from the snippets.\\n\\n"
    "**Step 2: Targeted Extraction (if needed)**\\n"
    "If information is still missing after search, use nimble_extract_content on\\n"
    "1-2 relevant URLs from the search results to get full page content.\\n\\n"
    'Return "Not found" for any field you cannot determine.\\n'
    "Be concise and factual. Do not hallucinate information."
)

system_prompt = custom_prompt if custom_prompt else default_prompt

try:
    tools = [
        _make_truncated_tool(NimbleSearchTool(api_key=nimble_api_key)),
        _make_truncated_tool(NimbleExtractTool(api_key=nimble_api_key, output_format="plain_text")),
    ]
    agent = create_agent(model=llm, tools=tools, system_prompt=system_prompt)

    # Build query
    context = ", ".join([f"{k}: {v}" for k, v in input_data.items()])
    query = (
        f"Find the following information: [{', '.join(output_cols)}] "
        f"for the entity described by: {context}. "
        f"Return a JSON object with these exact keys: {json.dumps(output_cols)}. "
        f'Use "Not found" for any field you cannot determine.'
    )

    # Invoke agent
    result = agent.invoke({"messages": [{"role": "user", "content": query}]})

    # Extract last AI message
    output_text = ""
    for msg in reversed(result.get("messages", [])):
        if hasattr(msg, "type") and msg.type == "ai" and hasattr(msg, "content"):
            content = msg.content
            if isinstance(content, str):
                output_text = content
            elif isinstance(content, list):
                output_text = "\\n".join(
                    b["text"] if isinstance(b, dict) and b.get("type") == "text"
                    else str(b) for b in content if isinstance(b, (dict, str))
                )
            else:
                output_text = str(content)
            break

    if not output_text:
        output_text = str(result)

    # Parse JSON from response (balanced brace matching for nested JSON)
    depth = 0
    start = -1
    for i, ch in enumerate(output_text):
        if ch == '{':
            if depth == 0:
                start = i
            depth += 1
        elif ch == '}':
            depth -= 1
            if depth == 0 and start != -1:
                try:
                    parsed = json.loads(output_text[start:i+1])
                    enriched = {}
                    parsed_lower = {k.lower().replace(" ", "_"): v for k, v in parsed.items()}
                    for col in output_cols:
                        col_key = col.lower().replace(" ", "_")
                        val = parsed_lower.get(col_key, parsed.get(col, "Not found"))
                        enriched[col] = json.dumps(val) if isinstance(val, (list, dict)) else str(val)
                    return json.dumps(enriched)
                except json.JSONDecodeError:
                    start = -1

    fallback = {col: "Not found" for col in output_cols}
    fallback["_raw"] = output_text[:500]
    return json.dumps(fallback)

except Exception as e:
    return json.dumps({"error": str(e)})
$$
"""

spark.sql(sql)
print(f"Internal UDF created: {CATALOG}.{SCHEMA}._nimble_enrich_internal")

# --- Step 3b: Create the public SQL wrappers ---

DATABRICKS_HOST = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiUrl().getOrElse(None)

# Wrapper 1: ChatDatabricks (Model Serving)
spark.sql(f"""
CREATE OR REPLACE FUNCTION {CATALOG}.{SCHEMA}.nimble_enrich(
    input_json     STRING,
    output_columns STRING,
    custom_prompt  STRING DEFAULT NULL
)
RETURNS STRING
COMMENT 'Enriches a row with live web data using a LangChain agent with Nimble tools + ChatDatabricks (Model Serving). Credentials injected automatically.'
RETURN {CATALOG}.{SCHEMA}._nimble_enrich_internal(
    input_json,
    output_columns,
    secret('nimble', 'nimble_api_key'),
    'databricks',
    secret('nimble', 'databricks_token'),
    '{DATABRICKS_HOST}',
    custom_prompt
)
""")
print(f"Public UDF created: {CATALOG}.{SCHEMA}.nimble_enrich (ChatDatabricks)")

# Wrapper 2: ChatAnthropic (Anthropic API)
spark.sql(f"""
CREATE OR REPLACE FUNCTION {CATALOG}.{SCHEMA}.nimble_enrich_anthropic(
    input_json     STRING,
    output_columns STRING,
    custom_prompt  STRING DEFAULT NULL
)
RETURNS STRING
COMMENT 'Enriches a row with live web data using a LangChain agent with Nimble tools + ChatAnthropic (Anthropic API). Credentials injected automatically.'
RETURN {CATALOG}.{SCHEMA}._nimble_enrich_internal(
    input_json,
    output_columns,
    secret('nimble', 'nimble_api_key'),
    'anthropic',
    secret('nimble', 'anthropic_api_key'),
    '',
    custom_prompt
)
""")
print(f"Public UDF created: {CATALOG}.{SCHEMA}.nimble_enrich_anthropic (ChatAnthropic)")

# --- Step 4: Verify ---

display(spark.sql(f"DESCRIBE FUNCTION EXTENDED {CATALOG}.{SCHEMA}.nimble_enrich"))

# --- Step 5: Quick Test ---

result = spark.sql(f"""
SELECT {CATALOG}.{SCHEMA}.nimble_enrich(
    '{{"company_name": "Anthropic", "website": "anthropic.com"}}',
    '["ceo", "year_founded", "headquarters"]'
) AS enriched
""")
display(result)
