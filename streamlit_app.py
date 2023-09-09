import streamlit as st
import pandas as pd

st.title("Guardian News Classifier")

uploaded_file = st.sidebar.file_uploader(
                                        "Upload your input CSV file",
                                        type=["csv"]
                                        )
if uploaded_file is not None:
    # Read the uploaded file
    df = pd.read_csv(uploaded_file)
    out = df.drop(['id'], axis=1)
    st.write(out)
else:
    st.write('Awaiting CSV file to be uploaded.')