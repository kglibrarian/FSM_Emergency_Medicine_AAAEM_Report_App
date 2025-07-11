import streamlit as st
import datetime
import io
from logic import run_all

st.set_page_config(layout="wide")
st.title("Publication Metrics Generator")

df_1_file = st.file_uploader("Upload Publication Report (df_1)", type="csv")
df_2_file = st.file_uploader("Upload Author ID Report (df_2)", type="csv")

col1, col2 = st.columns(2)
with col1:
    start_date = st.date_input("Start Date", datetime.date(2024, 7, 1))
with col2:
    end_date = st.date_input("End Date", datetime.date(2025, 6, 30))

if st.button("Run Analysis"):
    if not (df_1_file and df_2_file):
        st.error("Please upload both files.")
    else:
        # Convert binary streams to text streams (UTF-8 decoded)
        df_1_text = io.StringIO(df_1_file.getvalue().decode("utf-8"))
        df_2_text = io.StringIO(df_2_file.getvalue().decode("utf-8"))

        with st.spinner("Processing..."):
            df_8_list, df_11_list = run_all(df_1_text, df_2_text, start_date, end_date)

        st.success("Done!")

        # Convert to downloadable CSV text
        df_8_csv = "\n".join([",".join(map(str, row.values())) for row in [df_8_list[0].keys()] + df_8_list])
        df_11_csv = "\n".join([",".join(map(str, row.values())) for row in [df_11_list[0].keys()] + df_11_list])

        st.download_button("Download df_8 (Authorship Summary)", df_8_csv, "df_8.csv")
        st.download_button("Download df_11 (Total Publications)", df_11_csv, "df_11.csv")

        st.subheader("Preview df_8")
        st.dataframe(df_8_list)

        st.subheader("Preview df_11")
        st.dataframe(df_11_list)
