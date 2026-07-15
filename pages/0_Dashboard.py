import pandas as pd
import streamlit as st
from core import store
from core.ui import inject_global_css, sidebar_brand, status_badge

inject_global_css(); sidebar_brand()
st.title("🩺 DataDoctor AI")
st.caption("Enterprise pipeline operations, observability, repair, analytics, and AI-assisted diagnosis.")
pipelines, runs = store.load_pipelines(), store.load_runs()
succeeded = sum(r["status"] == "SUCCEEDED" for r in runs)
c1,c2,c3,c4=st.columns(4)
c1.metric("Pipelines",len(pipelines)); c2.metric("Total Runs",len(runs)); c3.metric("Failed Runs",sum(r["status"]=="FAILED" for r in runs)); c4.metric("Success Rate",f"{succeeded/len(runs)*100:.0f}%" if runs else "—")
st.divider(); st.subheader("Recent pipeline runs")
if runs:
    rows=[]
    for r in sorted(runs,key=lambda x:x["started_at"],reverse=True)[:20]:
        rows.append({"Run ID":r["id"],"Pipeline":r["pipeline_name"],"Status":r["status"],"Started":r["started_at"][:19].replace("T"," "),"Steps":len(r.get("step_runs",[])),"Rows processed":sum(x.get("rows_processed") or 0 for x in r.get("step_runs",[]))})
    st.dataframe(pd.DataFrame(rows),use_container_width=True,hide_index=True)
else: st.info("No runs yet. Open Pipeline Builder, save a template, and run it.")
st.subheader("Registered pipelines")
if pipelines:
    st.dataframe(pd.DataFrame([{"Pipeline":p["name"],"Description":p.get("description","") ,"Steps":len(p.get("steps",[])),"Created":p.get("created_at","")[:19].replace("T"," ")} for p in pipelines]),use_container_width=True,hide_index=True)
else: st.info("No pipelines created yet.")
st.caption("Execution is simulated for zero-cost deployment; orchestration, dependencies, retries, repair, logs, analytics, and agent workflows are functional.")
