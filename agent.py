"""
Databricks Pipeline Observability Agent
LangGraph ReAct + Groq llama-3.3-70b-versatile consuming the MCP server via stdio.

Usage:
    python agent.py
    python agent.py "why did the gold aggregation job fail this morning?"

LangSmith tracing activates automatically when LANGCHAIN_API_KEY is set in .env.
"""

from __future__ import annotations

import asyncio
import os
import sys

from dotenv import load_dotenv
from langchain_core.messages import HumanMessage
from langchain_groq import ChatGroq
from langchain_mcp_adapters.client import MultiServerMCPClient
from langgraph.prebuilt import create_react_agent
from pydantic import SecretStr

load_dotenv()


# ── LangSmith tracing ────────────────────────────────────────────────────────
# LangChain reads these env vars automatically — no extra code needed here.
# Add to your .env file to enable:
#
#   LANGCHAIN_TRACING_V2=true
#   LANGCHAIN_API_KEY=<your langsmith key>
#   LANGCHAIN_PROJECT=databricks-observability


# ── LLM ─────────────────────────────────────────────────────────────────────

def _build_llm() -> ChatGroq:
    api_key = os.environ.get("GROQ_API_KEY")
    if not api_key:
        raise EnvironmentError("GROQ_API_KEY is not set — add it to your .env file")
    return ChatGroq(
        model="llama-3.3-70b-versatile",
        temperature=0,
        api_key=SecretStr(api_key),   # ChatGroq requires SecretStr, not a plain str
    )


# ── System prompt ────────────────────────────────────────────────────────────

SYSTEM_PROMPT = """\
You are a Databricks lakehouse reliability engineer.
Your job is to diagnose pipeline health issues using the available tools.

Investigation strategy (follow this order):
1. Start with the symptom the user described — check job runs or query history first.
2. Drill into the relevant cluster or table for the root cause.
3. ALWAYS finish with get_lineage to quantify downstream blast radius.

You have a maximum of 3 tool-call rounds. Be efficient — choose the most
relevant tools per round rather than calling everything.

Respond ONLY in this exact format — no extra text before or after:

## What Happened
<1-2 sentences: the observable symptom with specific names and timestamps>

## Root Cause
<the specific technical cause with evidence from tool results>

## Blast Radius
<which downstream tables, jobs, or dashboards are affected — include blast_radius_score>

## Recommended Fix
<concrete, ordered steps to resolve the issue>
"""


# ── MCP server config ────────────────────────────────────────────────────────
# SSE transport: connects to the already-running server.py service.
# Start the server first:  python server.py
# Then run the agent:      python agent.py

MCP_CONFIG = {
    "databricks": {
        "transport": "sse",
        "url": "http://127.0.0.1:8000/sse",
        "headers": {"Authorization": f"Bearer {os.environ.get('MCP_API_KEY', '')}"},
    }
}


# ── Agent runner ─────────────────────────────────────────────────────────────

async def run_query(user_query: str) -> str:
    """Run one observability query and return the structured diagnosis."""
    client = MultiServerMCPClient(MCP_CONFIG)

    # get_tools() spawns server.py and returns LangChain-compatible wrappers
    # built from the @mcp.tool() definitions — name, description, input schema.
    tools = await client.get_tools()

    agent = create_react_agent(
        model=_build_llm(),
        tools=tools,
        prompt=SYSTEM_PROMPT,
    )

    # recursion_limit caps node transitions in the LangGraph state machine.
    # ReAct alternates: [agent] → [tools] → [agent] → ...
    # Each round = 2 transitions. 3 rounds + 2 buffer = 8.
    result = await agent.ainvoke(
        {"messages": [HumanMessage(content=user_query)]},
        config={"recursion_limit": 8},
    )

    # Last message in state is always the final agent response.
    return result["messages"][-1].content


# ── Entry point ──────────────────────────────────────────────────────────────

DEFAULT_QUERY = (
    "Why did the aggregate_gold job fail this morning "
    "and what is the blast radius?"
)

if __name__ == "__main__":
    query = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_QUERY

    print(f"\nQuery: {query}")
    print("─" * 60)

    answer = asyncio.run(run_query(query))
    print(answer)
