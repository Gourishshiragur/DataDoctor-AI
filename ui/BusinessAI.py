"""ui/BusinessAI.py — natural-language SQL generation and a chat-style Q&A assistant
grounded in the Gold layer (with RAG over run history for "why" questions)."""
import streamlit as st

from ai import business_assistant, sql_generator
from ai.provider_router import get_active_provider
from config.settings import load_settings
from rag.retriever import retrieve
from storage import router as db


def render():
    st.title("🤖 Business AI")
    active_provider = get_active_provider()
    st.caption(f"Active AI provider: **{active_provider}**" + (" (offline rule-based fallback)" if active_provider == "offline" else ""))

    gold_tables = db.list_tables("gold")
    if not gold_tables:
        st.info("No Gold datasets yet. Run a pipeline in **Pipeline Studio** first.")
        return

    table_name = st.selectbox("Gold dataset", gold_tables)
    gold_df = db.read_table("gold", table_name)

    tab1, tab2, tab3 = st.tabs(["💬 Ask a question", "🧾 SQL Generator", "📄 Summary"])

    with tab1:
        st.dataframe(gold_df.head(10), use_container_width=True)
        question = st.text_input("Ask a business question about this dataset", placeholder="e.g. which category has the highest total revenue?")
        if st.button("Ask", key="ask_btn") and question:
            with st.spinner("Thinking..."):
                kpis = {c: str(gold_df[c].iloc[0]) for c in gold_df.columns[:5]} if len(gold_df) else {}
                answer = business_assistant.ask(question, kpis, gold_df.head(10).to_markdown())
                top_k = load_settings()["ai"]["rag"]["top_k"]
                context = retrieve(question, top_k=top_k)
            st.markdown(answer["answer"])
            st.caption(f"via {answer['provider']}")
            if context:
                with st.expander("📚 Related context (RAG over run history & docs)"):
                    for c in context:
                        st.write(f"**{c['source']}**: {c['text']}")

    with tab2:
        nl_query = st.text_input("Describe the query you want", placeholder="e.g. top 10 rows by total sales")
        if st.button("Generate SQL", key="sql_btn") and nl_query:
            with st.spinner("Generating SQL..."):
                result = sql_generator.generate_sql(nl_query, gold_df, table_name)
            st.code(result["sql"], language="sql")
            st.caption(f"via {result['provider']}")
            try:
                out = sql_generator.run_query(result["sql"], gold_df, table_name)
                st.dataframe(out, use_container_width=True)
            except Exception as e:  # noqa: BLE001
                st.error(f"Query failed to execute: {e}")

    with tab3:
        if st.button("Generate business summary", key="summary_btn"):
            with st.spinner("Summarizing..."):
                numeric_cols = gold_df.select_dtypes(include="number").columns[:6]
                kpis = {c: round(float(gold_df[c].sum()), 2) for c in numeric_cols}
                summary = business_assistant.summarize_kpis(kpis)
            st.markdown(summary["summary"])
            st.caption(f"via {summary['provider']}")
