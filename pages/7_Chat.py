"""
Conversational AI — multi-turn chat with context memory.

Demonstrates:
  - Multi-turn LLM conversation (full message history sent each turn)
  - RAG context injection: each user message is checked against incident
    memory and relevant past incidents are injected as system context
  - Streaming responses token-by-token
  - Domain-aware system prompt: knows your pipeline stack, data model,
    and can answer questions about Spark, Delta Lake, SCD2, ADF etc.
"""

import json

import requests
import streamlit as st

from core.ai_assistant import _get_api_key, ANTHROPIC_API_URL, MODEL
from core.rag_memory import incident_count, retrieve_similar
from core.ui import inject_global_css, sidebar_brand


st.set_page_config(
    page_title="Chat · DataDoctor AI",
    page_icon="💬",
    layout="wide",
)

inject_global_css()
sidebar_brand()

st.title("💬 Conversational AI")

st.caption(
    "Multi-turn chat with your pipeline domain. Ask about Spark, Delta Lake, SCD2, "
    "ADF patterns, or paste an error and troubleshoot interactively. "
    "Past incidents from RAG memory are injected as context automatically."
)


SYSTEM_PROMPT = (
    "You are DataDoctor AI, a senior data engineering assistant specializing in "
    "Azure Databricks, PySpark, Delta Lake, ADF, and Spark Structured Streaming. "
    "You know the Bronze-Silver-Gold Medallion architecture deeply. "
    "When the user shares an error or asks for help debugging, be specific and practical — "
    "give working PySpark/SQL code, not vague advice. "
    "When asked about architecture decisions, explain trade-offs clearly. "
    "Keep responses concise but complete."
)


# Initialize chat history.
if "chat_history" not in st.session_state:
    st.session_state.chat_history = []


# Sidebar controls.
with st.sidebar:
    st.divider()

    st.markdown("**Chat settings**")

    rag_enabled = st.toggle(
        "Inject RAG context",
        value=True,
        help=(
            "Retrieve similar past incidents and inject them "
            "before each response"
        ),
    )

    stream_enabled = st.toggle(
        "Stream responses",
        value=True,
    )

    # Do not call incident_count() while opening the page.
    # This allows the Chat UI and input field to render immediately.
    st.caption("RAG memory loads after you send a message")

    if st.button("Clear chat"):
        st.session_state.chat_history = []
        st.rerun()


# Render existing chat history.
for msg in st.session_state.chat_history:
    with st.chat_message(msg["role"]):
        st.markdown(msg["content"])


# Render the input before initializing RAG/vector memory.
user_input = st.chat_input(
    "Ask about Spark, Delta Lake, SCD2, paste an error..."
)


if user_input:

    # Add and display the user's message first.
    st.session_state.chat_history.append(
        {
            "role": "user",
            "content": user_input,
        }
    )

    with st.chat_message("user"):
        st.markdown(user_input)


    # RAG: load incident memory only after a message is submitted.
    rag_context = ""
    mem_count = 0

    if rag_enabled:

        try:

            with st.spinner("Searching incident memory..."):

                mem_count = incident_count()

                if mem_count > 0:

                    similar = retrieve_similar(
                        user_input,
                        k=2,
                    )

                else:

                    similar = []


            if similar:

                parts = []

                for r in similar:

                    parts.append(
                        f"Past incident "
                        f"(similarity {r.similarity_score:.2f}):\n"
                        f"  Error: {r.incident.error_message}\n"
                        f"  Root cause: {r.incident.root_cause}\n"
                        f"  Fix: {r.incident.fix_applied}"
                    )


                rag_context = (
                    "\n\nRelevant past incidents "
                    "from RAG memory:\n"
                    + "\n---\n".join(parts)
                )


        except Exception:

            # RAG failure must not stop Chat.
            rag_context = ""
            mem_count = 0

            st.warning(
                "RAG incident memory is temporarily unavailable. "
                "Continuing the conversation without RAG context."
            )


    system = SYSTEM_PROMPT + rag_context


    # Get Claude API key.
    api_key = _get_api_key()


    with st.chat_message("assistant"):

        if not api_key:

            response = (
                "⚠️ No ANTHROPIC_API_KEY configured — "
                "I can't answer conversationally without it. "
                "Add your key in Streamlit → Settings → Secrets as "
                "`ANTHROPIC_API_KEY = 'sk-ant-...'`. "
                "The AI Agent and Code Assistant pages have "
                "rule-based fallbacks; the chat page needs a live API key."
            )

            st.markdown(response)

            st.session_state.chat_history.append(
                {
                    "role": "assistant",
                    "content": response,
                }
            )


        else:

            messages = [
                {
                    "role": m["role"],
                    "content": m["content"],
                }
                for m in st.session_state.chat_history
            ]


            try:

                # Streaming Claude response.
                if stream_enabled:

                    resp = requests.post(
                        ANTHROPIC_API_URL,
                        headers={
                            "x-api-key": api_key,
                            "anthropic-version": "2023-06-01",
                            "content-type": "application/json",
                        },
                        json={
                            "model": MODEL,
                            "max_tokens": 1000,
                            "system": system,
                            "messages": messages,
                            "stream": True,
                        },
                        timeout=60,
                        stream=True,
                    )


                    resp.raise_for_status()


                    placeholder = st.empty()

                    full = ""


                    for raw_line in resp.iter_lines():

                        if not raw_line:
                            continue


                        line = (
                            raw_line.decode("utf-8")
                            if isinstance(raw_line, bytes)
                            else raw_line
                        )


                        if not line.startswith("data: "):
                            continue


                        payload = line[6:]


                        if payload.strip() == "[DONE]":
                            break


                        try:

                            event = json.loads(payload)

                        except json.JSONDecodeError:

                            continue


                        if event.get("type") == "content_block_delta":

                            delta = event.get(
                                "delta",
                                {},
                            )


                            if delta.get("type") == "text_delta":

                                full += delta.get(
                                    "text",
                                    "",
                                )


                                placeholder.markdown(
                                    full + "▌"
                                )


                    placeholder.markdown(full)


                    st.session_state.chat_history.append(
                        {
                            "role": "assistant",
                            "content": full,
                        }
                    )


                    if rag_context:

                        st.caption(
                            f"📚 RAG context injected from "
                            f"{mem_count} incident(s)"
                        )


                # Non-streaming Claude response.
                else:

                    resp = requests.post(
                        ANTHROPIC_API_URL,
                        headers={
                            "x-api-key": api_key,
                            "anthropic-version": "2023-06-01",
                            "content-type": "application/json",
                        },
                        json={
                            "model": MODEL,
                            "max_tokens": 1000,
                            "system": system,
                            "messages": messages,
                        },
                        timeout=30,
                    )


                    resp.raise_for_status()


                    data = resp.json()


                    text = "".join(
                        block.get(
                            "text",
                            "",
                        )
                        for block in data.get(
                            "content",
                            [],
                        )
                        if block.get("type") == "text"
                    )


                    st.markdown(text)


                    st.session_state.chat_history.append(
                        {
                            "role": "assistant",
                            "content": text,
                        }
                    )


            except Exception as e:

                err = f"API error: {e}"


                st.error(err)


                st.session_state.chat_history.append(
                    {
                        "role": "assistant",
                        "content": err,
                    }
                )