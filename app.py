import streamlit as st
import pandas as pd
from datetime import datetime
from logic import PublicationAnalyzer

# Configure Streamlit page
st.set_page_config(
    page_title="Emergency Medicine Publication Analysis",
    page_icon="📚",
    layout="wide"
)

st.title("📚 Emergency Medicine Publication Analysis")
st.markdown("Upload your data files and configure analysis parameters to generate AAAEM reports.")

# Configuration section
st.header("🔧 Configuration")

# Get API key from secrets
try:
    scopus_api_key = st.secrets["SCOPUS_API_KEY"]
    st.success("✅ Scopus API key loaded from secrets")
except KeyError:
    st.error("❌ Scopus API key not found in secrets. Please add SCOPUS_API_KEY to your Streamlit secrets.")
    scopus_api_key = None

# Date range selector
col1, col2 = st.columns(2)
with col1:
    start_date = st.date_input(
        "Publication Start Date",
        value=datetime(2024, 7, 1),
        help="Start date for publication analysis period"
    )
with col2:
    end_date = st.date_input(
        "Publication End Date", 
        value=datetime(2025, 6, 30),
        help="End date for publication analysis period"
    )

# File upload section
st.header("📁 File Upload")

col1, col2 = st.columns(2)

with col1:
    st.subheader("Publication Report")
    uploaded_file_1 = st.file_uploader(
        "Upload Elements Publication Report (CSV)",
        type=['csv'],
        help="Upload the Emergency Medicine Elements Publication Report CSV file"
    )

with col2:
    st.subheader("Author ID Report")
    uploaded_file_2 = st.file_uploader(
        "Upload Elements Author ID Report (CSV)",
        type=['csv'],
        help="Upload the Emergency Medicine Elements Author ID Report CSV file"
    )

# Main app logic
if uploaded_file_1 is not None and uploaded_file_2 is not None and scopus_api_key:
    try:
        # Load data
        df1 = pd.read_csv(uploaded_file_1, encoding='utf-8')
        df2 = pd.read_csv(uploaded_file_2, encoding='utf-8')
        
        st.success(f"✅ Files loaded successfully!")
        st.write(f"📊 Publication Report: {df1.shape[0]} rows, {df1.shape[1]} columns")
        st.write(f"👥 Author ID Report: {df2.shape[0]} rows, {df2.shape[1]} columns")
        
        # Process data button
        if st.button("🚀 Process Data", type="primary"):
            
            # Create status container for updates
            status_container = st.empty()
            
            def update_status(message):
                status_container.write(message)
            
            with st.spinner("Processing data... This may take several minutes."):
                analyzer = PublicationAnalyzer(scopus_api_key)
                
                try:
                    df_faculty_summary, df_pub_summary = analyzer.process_data(
                        df1, df2, start_date, end_date, status_callback=update_status
                    )
                    
                        # Store results in session state to persist downloads
                        st.session_state.df_faculty_summary = df_faculty_summary
                        st.session_state.df_pub_summary = df_pub_summary
                        st.session_state.analysis_complete = True
                    if df_faculty_summary is not None and df_pub_summary is not None:
                        # Store results in session state to persist downloads
                        st.session_state.df_faculty_summary = df_faculty_summary
                        st.session_state.df_pub_summary = df_pub_summary
                        st.session_state.analysis_complete = True
                        
                        status_container.success("✅ Analysis completed successfully!")
                        
                    else:
                        st.error("❌ Analysis failed. Please check your data and try again.")
                        
                except Exception as e:
                    st.error(f"❌ An error occurred during processing: {str(e)}")
                    st.write("Please check your API key and data files.")

# Display results if analysis is complete (this will persist across reruns)
if hasattr(st.session_state, 'analysis_complete') and st.session_state.analysis_complete:
    df_faculty_summary = st.session_state.df_faculty_summary
    df_pub_summary = st.session_state.df_pub_summary
    
    # Display results
    st.header("📊 Results")
    
    # Faculty Summary (df_8 equivalent)
    st.subheader("👥 Faculty Publication Summary")
    st.dataframe(df_faculty_summary, use_container_width=True)
    
    # Publication Assignment Summary (df_11 equivalent)
    st.subheader("📚 Publication Assignment Summary")
    st.dataframe(df_pub_summary, use_container_width=True)
    
    # Download buttons - these will persist now
    st.header("⬇️ Download Results")
    
    col1, col2 = st.columns(2)
    
    with col1:
        faculty_csv = df_faculty_summary.to_csv(index=False)
        st.download_button(
            label="📋 Download Faculty Summary",
            data=faculty_csv,
            file_name=f"faculty_summary_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
            mime="text/csv",
            key="download_faculty"  # Unique key to prevent conflicts
        )
    
    with col2:
        pub_csv = df_pub_summary.to_csv(index=False)
        st.download_button(
            label="📊 Download Publication Summary",
            data=pub_csv,
            file_name=f"publication_summary_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
            mime="text/csv",
            key="download_pubs"  # Unique key to prevent conflicts
        )
    
    # Summary statistics
    st.header("📈 Summary Statistics")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        total_faculty = len(df_faculty_summary)
        st.metric("Total Faculty", total_faculty)
    
    with col2:
        total_pubs = df_pub_summary['Total Publications'].sum()
        st.metric("Total Publications", total_pubs)
    
    with col3:
        faculty_with_pubs = (df_pub_summary['Total Publications'] > 0).sum()
        st.metric("Faculty with Publications", faculty_with_pubs)
    
    with col4:
        avg_pubs = df_pub_summary['Total Publications'].mean()
        st.metric("Avg Publications per Faculty", f"{avg_pubs:.1f}")
    
    # Clear results button
    if st.button("🔄 Clear Results and Start Over", key="clear_results"):
        for key in ['df_faculty_summary', 'df_pub_summary', 'analysis_complete']:
            if key in st.session_state:
                del st.session_state[key]
        st.rerun()
    
    except Exception as e:
        st.error(f"❌ Error loading files: {str(e)}")

elif uploaded_file_1 is None or uploaded_file_2 is None:
    st.info("📁 Please upload both CSV files to proceed.")
    
elif not scopus_api_key:
    st.info("🔑 Scopus API key not available. Please configure it in Streamlit secrets.")

# Footer
st.markdown("---")
st.markdown("*Emergency Medicine Publication Analysis Tool - Built with Streamlit*")