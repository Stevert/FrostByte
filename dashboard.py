import streamlit as st
import pyarrow.ipc as ipc
import pyarrow as pa
import pandas as pd
import os

st.title("Iceberg Query Results Dashboard")

storage_path = st.text_input("Enter path to query results:", value="/tmp/results")

if os.path.isdir(storage_path):
    arrow_files = [f for f in os.listdir(storage_path) if f.endswith(".arrow")]

    selected_file = st.selectbox("Select a result file to view:", arrow_files)

    if selected_file:
        file_path = os.path.join(storage_path, selected_file)
        with pa.memory_map(file_path, "r") as source:
            reader = ipc.RecordBatchFileReader(source)
            table = reader.read_all()
            df = table.to_pandas()
            st.dataframe(df)
            st.write("### Plotting Preview")
            if not df.empty:
                cols = df.columns.tolist()
                x = st.selectbox("X Axis", cols)
                y = st.selectbox("Y Axis", cols)
                chart = st.line_chart(df[[x, y]])
else:
    st.warning("Invalid result path.")
