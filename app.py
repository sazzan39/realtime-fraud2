import streamlit as st
import pandas as pd

st.set_page_config(layout="wide")
st.title("🚨 Real-Time Fraud Detection")

# auto refresh
st.experimental_rerun()

try:
    df = pd.read_csv("results.csv", names=["prob", "status"])

    col1, col2 = st.columns(2)

    col1.metric("Total Transactions", len(df))
    col2.metric("Fraud Detected", (df["status"] == "FRAUD").sum())

    st.divider()

    col3, col4 = st.columns(2)

    with col3:
        st.subheader("Fraud vs Normal")
        st.bar_chart(df["status"].value_counts())

    with col4:
        st.subheader("Probability Trend")
        st.line_chart(df["prob"])

    # 🚨 latest alert
    last = df.iloc[-1]

    if last["status"] == "FRAUD":
        st.error(f"🚨 FRAUD DETECTED → {last['prob']:.2f}")
    else:
        st.success(f"✅ NORMAL → {last['prob']:.2f}")

except:
    st.warning("Waiting for data...")