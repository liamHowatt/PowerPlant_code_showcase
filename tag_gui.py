import push_latest_json
import converter
import streamlit as st
import pandas as pd
import numpy as np
from io import StringIO
from db_credentials import uri
import sqlalchemy
import sys
import base64


CONVERTER_DIR = "/home/farmer/local-server-dilution-reconfig/modbus_converter_output/"


def make_link(file_content, label, filename="modbus_mapping.ccwmod"):
    b64 = base64.b64encode(file_content.encode()).decode()
    return f'<a href="data:file/ccwmod;base64,{b64}" download="{filename}">{label}</a>'


tag_csv_file = st.file_uploader("Upload tag mapping CSV")

plc_id = st.number_input("PLC ID to use", min_value=1, value=1, step=1)

cols = st.beta_columns(2)

do_it_btn = cols[0].button("1. Do it")
reactivate_btn = cols[1].button(f"2. Reactivate NodeRED for PLC #{plc_id}")

if reactivate_btn:
    push_latest_json.main(plc_id)
    st.success("Success")
    st.stop()
if not do_it_btn:
    st.stop()

if tag_csv_file is None:
    st.warning("Upload a file")
    st.stop()

tag_df = pd.read_csv(tag_csv_file)
st.write(tag_df)
columns = ["tag_name", "data_type", "plc_id", "logged", "log_rate_ms"]
if not all(np.isin(columns, tag_df.columns)):
    st.warning("CSV needs to include these columns: " + str(columns))
    st.stop()
tag_df = tag_df[tag_df.plc_id == plc_id]

db = sqlalchemy.create_engine(uri)
with db.begin() as cursor:
    cursor.execute(
        f"""
        DELETE FROM tags
        WHERE plc_id={plc_id}
        ;
        """
    )
    tag_df.to_sql(
        "tags",
        cursor,
        if_exists="append",
        index=False
    )
    st.markdown('updated mysql "tags" table')

stdout = sys.stdout
sys.stdout = StringIO()
converter.main(specific_plc=plc_id)
captured_stdout = sys.stdout
sys.stdout = stdout
del stdout
captured_stdout.seek(0)
st.text(captured_stdout.read())
st.markdown("generated modbus mapping")

push_latest_json.push_to_nodered("[]", plc_id)

latest_dir = push_latest_json.find_latest()

modbus_map_f = open(
    CONVERTER_DIR + latest_dir +
    f"/plc{plc_id}_ccw_modbus_map.ccwmod",
    "r"
)
st.markdown(
    make_link(
        modbus_map_f.read(),
        "Download modbus CCWMOD file"
    ),
    unsafe_allow_html=True
)
modbus_map_f.close()

st.warning(
    f'NodeRED for PLC #{plc_id} has been disabled while you\'re updating the PLC. '
    f'Click the button "2. Reactivate NodeRED for PLC #{plc_id}" at the top of this '
    'page to reactivate NodeRED.'
)