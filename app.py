import streamlit as st
import pandas as pd
import datetime
from logic import run_all

st.set_page_config(layout="wide")
st.title("Publication Metrics Generator")

df_1_file = st.file_uploader("Upload Author ID Report (df_1)", type="csv")
df_2_file = st.file_uploader("Upload Publication Report (df_2)", type="csv")

col1, col2 = st.columns(2)
with col1:
    start_date = st.date_input("Start Date", datetime.date(2024, 7, 1))
with col2:
    end_date = st.date_input("End Date", datetime.date(2025, 6, 30))

if st.button("Run Analysis"):
    if not (df_1_file and df_2_file):
        st.error("Please upload both CSV files.")
    else:
        with st.spinner("Processing..."):
            df_8_list, df_11_list = run_all(df_1_file, df_2_file, start_date, end_date)

        if not df_8_list or not df_11_list:
            st.warning("No data returned.")
        else:
            # Convert to DataFrames for Streamlit display and download
            df_8 = pd.DataFrame(df_8_list)
            df_11 = pd.DataFrame(df_11_list)

            st.success("Done!")

            st.download_button("📥 Download df_8 (Authorship Summary)", df_8.to_csv(index=False), "df_8.csv")
            st.download_button("📥 Download df_11 (Total Publications)", df_11.to_csv(index=False), "df_11.csv")

            st.subheader("Preview df_8")
            st.dataframe(df_8)

            st.subheader("Preview df_11")
            st.dataframe(df_11)
