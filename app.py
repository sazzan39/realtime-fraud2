import streamlit as st
import pandas as pd
import time


st.set_page_config(page_title="Fraud Detection", layout="wide")


st.markdown("""
<style>
body {
    background-color: #FFFFFF;
}
.metric-card {
    background-color: #1c1f26;
    padding: 20px;
    border-radius: 12px;
    text-align: center;
}
.big-text {
    font-size: 28px;
    font-weight: bold;
}
.green {
    color: #00FFAA;
}
.red {
    color: #FF4B4B;
}
</style>
""", unsafe_allow_html=True)


st.markdown("Real-Time Fraud Detection System")

placeholder = st.empty()

while True:
    try:
        df = pd.read_csv("results.csv", names=["prob", "status"])

        total = len(df)
        fraud = (df["status"] == "FRAUD").sum()
        normal = (df["status"] == "NORMAL").sum()
        fraud_rate = (fraud / total * 100) if total > 0 else 0

        last = df.iloc[-1]

        with placeholder.container():

          
            if last["status"] == "FRAUD":
                st.markdown(f"""
                <div style="background:#ff4b4b;padding:15px;border-radius:10px;color:white;">
                🚨 <b>FRAUD DETECTED</b> — Probability: {last['prob']:.2f}
                </div>
                """, unsafe_allow_html=True)
            else:
                st.markdown(f"""
                <div style="background:#00c853;padding:15px;border-radius:10px;color:white;">
                Normal Transaction — Probability: {last['prob']:.2f}
                </div>
                """, unsafe_allow_html=True)

            st.markdown("---")

            
            col1, col2, col3, col4 = st.columns(4)

            col1.markdown(f"""
            <div class="metric-card">
                <div>Total</div>
                <div class="big-text">{total}</div>
            </div>
            """, unsafe_allow_html=True)

            col2.markdown(f"""
            <div class="metric-card">
                <div class="red">Fraud</div>
                <div class="big-text red">{fraud}</div>
            </div>
            """, unsafe_allow_html=True)

            col3.markdown(f"""
            <div class="metric-card">
                <div class="green">Normal</div>
                <div class="big-text green">{normal}</div>
            </div>
            """, unsafe_allow_html=True)

            col4.markdown(f"""
            <div class="metric-card">
                <div>Fraud Rate</div>
                <div class="big-text">{fraud_rate:.2f}%</div>
            </div>
            """, unsafe_allow_html=True)

            st.markdown("---")

          
            col5, col6 = st.columns(2)

            with col5:
                st.subheader("Distribution")
                st.bar_chart(df["status"].value_counts())

            with col6:
                st.subheader("Risk Trend")
                st.line_chart(df["prob"])

            st.markdown("---")

            
            st.subheader("🧾 Live Transactions")

            def highlight(val):
                color = "red" if val == "FRAUD" else "green"
                return f"color: {color}; font-weight: bold"

            styled = df.tail(12).style.map(highlight, subset=["status"])

            st.dataframe(styled, use_container_width=True)

    except:
        st.warning("⏳ Waiting for incoming transactions...")

    time.sleep(1)