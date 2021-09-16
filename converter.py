"""
reads the database 'tags' table for
tag names and their data types assuming they are
based on CCW
global variables

outputs two files.
1. a ccwmod file to be imported by CCW to
   automatically configure modbus settings
2. a json file for node-red
   AND it automatically pushes that json
   to node-red over mqtt
"""

import os
import time
import json
import mysql.connector
from db_credentials import db_credentials

os.chdir(os.path.dirname(os.path.abspath(__file__)))

def main(specific_plc=None):

    new_dir = "modbus_converter_output/" + time.strftime("%m-%d-%Y_%H.%M.%S")
    os.mkdir(new_dir)

    db = mysql.connector.connect(**db_credentials)
    plc_cur = db.cursor()
    tag_cur = db.cursor()

    plc_cur.execute(f"""
        SELECT DISTINCT p.node_red_id
        FROM tags t
        JOIN plc p ON p.plc_id=t.plc_id
        { ('WHERE p.node_red_id=' + str(specific_plc)) if specific_plc is not None else '' };
    """)

    for (plc_id, ) in plc_cur:

        print("parsing tags for plc", plc_id)

        tag_cur.execute(f"""
            SELECT tag_name, data_type, logged
            FROM tags
            WHERE plc_id={plc_id}
            ORDER BY plc_id, tag_id;
        """)

        coil_addr = 0
        holding_addr = 0

        # 1. ccwmod
        ccwmod_coils = []
        ccwmod_holds = []
        # 2. json
        # json has both coils and holds together
        json_parts = []
        # #. end

        for tag_name, data_type, logged in tag_cur:

            # 1. ccwmod
            ccwmod_part = """\
            <mapping variable="{0}" parent="Micro850" dataType="{1}" address="{2}">
            <MBVarInfo ElemType="{1}" SubElemType="Any" DataTypeSize="{3}" />
            </mapping>""".format(
                tag_name,
                {"b": "Bool", "i": "Int", "r": "Real"}[data_type],
                str(coil_addr + 1).zfill(6) if data_type == "b" else holding_addr + 400001,
                {"b": "1", "i": "2", "r": "4"}[data_type]
            )
            (ccwmod_coils if data_type == "b" else ccwmod_holds).append(ccwmod_part)
            
            # 2. json
            json_part = {
                "tag_name": tag_name,
                "fc": {
                    "read": 1 if data_type == "b" else 3,
                    "write": {"b":5, "i":6, "r":16}[data_type]
                },
                "address": coil_addr if data_type == "b" else holding_addr,
                "quantity": 2 if data_type == "r" else 1,
                "data_type": {"b":"Bool", "i":"Int", "r":"Real"}[data_type],
                "reported": logged != 3
            }
            # json_part = {
            #     "read": {
            #         "tag_name": tag_name,
            #         "fc": 1 if data_type == "b" else 3,
            #         "address": coil_addr if data_type == "b" else holding_addr,
            #         "quantity": 2 if data_type == "r" else 1
            #     },
            #     "write": {
            #         "tag_name": tag_name,
            #         "fc": {"b":5, "i":6, "r":16}[data_type],
            #         "address": coil_addr if data_type == "b" else holding_addr,
            #         "quantity": 2 if data_type == "r" else 1
            #     }
            # }
            json_parts.append(json_part)
            # #. end

            increment = 2 if data_type == "r" else 1
            if data_type == "b":
                coil_addr += increment
            else:
                holding_addr += increment

        # 1. ccwmod
        print("making CCW modbus file")
        ccwmod_out = """\
<modbusServer Version="2.0">
    <modbusRegister name="COILS">
{}
    </modbusRegister>
    <modbusRegister name="HOLDING_REGISTERS">
{}
    </modbusRegister>
</modbusServer>""".format(
            "\n".join(ccwmod_coils),
            "\n".join(ccwmod_holds)
        )
        with open(new_dir + f"/plc{plc_id}_ccw_modbus_map.ccwmod", "w") as f:
            f.write(ccwmod_out)

        # 2. json
        print("making node-red json file")
        json_str = json.dumps(json_parts)
        with open(new_dir + f"/plc{plc_id}_node-red_tag_table.json", "w") as f:
            f.write(json_str)
        # print("pushing json to node-red")
        # json_str = json_str.replace("\"", "\\\"")
        # os.system(f'mosquitto_pub -r -t "nodered/plc/tag_table/{plc_id}" -m "{json_str}"')
        # #. end

if __name__ == "__main__":
    main()
