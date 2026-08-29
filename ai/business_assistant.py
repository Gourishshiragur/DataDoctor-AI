"""ai/business_assistant.py — chat-style Q&A and narrative summaries over the Gold layer."""
from __future__ import annotations

from ai import prompt_library
from ai.provider_router import infer


def summarize_kpis(kpis: dict) -> dict:
    resp = infer(prompt_library.business_prompt(kpis), system=prompt_library.BUSINESS_SYSTEM_PROMPT)
    return {"summary": resp.text, "provider": resp.provider}


def ask(question: str, kpis: dict, gold_preview_md: str) -> dict:
    system = prompt_library.BUSINESS_SYSTEM_PROMPT
    prompt = (
        f"Gold layer KPIs:\n" + "\n".join(f"- {k}: {v}" for k, v in kpis.items())
        + f"\n\nGold table preview:\n{gold_preview_md}\n\nUser question: {question}\n\nAnswer:"
    )
    resp = infer(prompt, system=system)
    return {"answer": resp.text, "provider": resp.provider}


def suggest_pipeline_plan(columns_dtypes: dict) -> dict:
    resp = infer(
        prompt_library.pipeline_plan_prompt(columns_dtypes),
        system=prompt_library.PIPELINE_PLAN_SYSTEM_PROMPT,
    )
    return {"plan": resp.text, "provider": resp.provider}
