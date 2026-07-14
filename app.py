import streamlit as st

st.set_page_config(page_title="DataDoctor AI", page_icon="🩺", layout="wide")

pages = {
    "DataDoctor AI": [
        st.Page("pages/0_Dashboard.py", title="Dashboard", icon="🏠", default=True),
    ],
    "Pipeline Operations": [
        st.Page("pages/1_Pipeline_Builder.py", title="Pipeline Builder", icon="🛠️"),
        st.Page("pages/2_Monitor_and_Repair.py", title="Monitor & Repair", icon="📊"),
        st.Page("pages/4_Logs.py", title="Logs", icon="📜"),
        st.Page("pages/6_Analytics.py", title="Analytics", icon="📈"),
    ],
    "AI Operations": [
        st.Page("pages/3_AI_Code_Assistant.py", title="AI Code Assistant", icon="🤖"),
        st.Page("pages/5_AI_Agent.py", title="AI Agent Console", icon="🧠"),
        st.Page("pages/7_Chat.py", title="Conversational AI", icon="💬"),
    ],
}

pg = st.navigation(pages)
pg.run()
