"""
For reconfig V1
"""

import pandas
from collections import namedtuple
from typing import Tuple
from db_credentials import db_credentials
import mysql.connector
import datetime
import time
import paho.mqtt.publish as publish
from nr_funcs import float2arr
import json

RACK_ID = 1
UPDATE_RATE_S = 5

# relevant tag names
TAG_CUT_IN = "recipe_volume_cutin"
TAG_CUT_OUT = "recipe_volume_cutout"
TAG_CF = "recipe_EC"

Time_Unit = namedtuple("Time_Unit", ["keyword", "column_name", "second_mult"])
SECOND = Time_Unit("second", "Seconds Elapsed", 1)
MINUTE = Time_Unit("minute", "Minutes Elapsed", 60)
HOUR = Time_Unit("hour", "Hours Elapsed", 3600)
DAY = Time_Unit("day", "Days Elapsed", 86_400)
TIME_UNITS = [SECOND, MINUTE, HOUR, DAY]

class DatabaseError(Exception):
    pass
class ColumnError(Exception):
    pass

def get_recipe(file_path: str) -> pandas.DataFrame:
    return pandas.read_csv(f"~/local-server-dilution-reconfig/recipes/{file_path}.csv")

def determine_time_unit(df: pandas.DataFrame) -> Time_Unit:
    for time_unit in TIME_UNITS:
        if time_unit.column_name in df:
            return time_unit
    raise ColumnError("unable to find recipe time column")

def get_rack_info(rack_id: int, db) -> Tuple[datetime.datetime, int, str]:
    cursor = db.cursor()
    cursor.execute(
        f"""
        SELECT ra.start_timestamp, p.node_red_id, re.recipe_csv_file_path
        FROM rack ra
        JOIN recipe re ON ra.recipe_id = re.recipe_id
        JOIN plc p ON ra.plc_id = p.plc_id
        WHERE ra.rack_id = {rack_id}
        LIMIT 1;
        """
    )
    fetch = cursor.fetchall()
    db.commit()
    if not fetch:
        raise DatabaseError(f"no rack with id {rack_id}")
    if None in fetch[0]:
        raise DatabaseError(f"missing information related to rack {fetch[0]}")
    return fetch[0]

def find_soonest_row(recipe: pandas.DataFrame, t_delta: datetime.timedelta, time_unit: Time_Unit) -> int:
    for row in range(recipe.shape[0]):
        recipe_time = float(recipe[time_unit.column_name][row]) * time_unit.second_mult
        if recipe_time > t_delta.seconds + t_delta.days * DAY.second_mult:
            return row
    return None

def calculate_c2(cf, vf, c1, v1, v2):
    if v2 <= 0.0:
        return 0.0
    return ( (cf * vf) - (c1 * v1) ) / v2

def main():

    db =  mysql.connector.connect(**db_credentials)

    while True:

        print("getting rack info...")
        try:
            start_timestamp, plc_id, csv_file_path = get_rack_info(rack_id=RACK_ID, db=db)
        except DatabaseError as e:
            print("database error")
            print(e)
            print(f"trying again in {UPDATE_RATE_S} seconds")
            time.sleep(UPDATE_RATE_S)
            continue
        print(start_timestamp)
        print("reading recipe...")
        try:
            recipe: pandas.DataFrame = get_recipe(csv_file_path)
        except FileNotFoundError as e:
            print("csv not found where expected")
            print(e)
            print(f"trying again in {UPDATE_RATE_S} seconds")
            time.sleep(UPDATE_RATE_S)
            continue
        print("finding time units...")
        try:
            time_unit: Time_Unit = determine_time_unit(recipe)
        except ColumnError as e:
            print(e)
            print(f"trying again in {UPDATE_RATE_S} seconds")
            time.sleep(UPDATE_RATE_S)
            continue

        print("looping through recipe")
        while True:
            time_since = datetime.datetime.now() - start_timestamp
            soonest_row = find_soonest_row(recipe, time_since, time_unit)
            if soonest_row is None:
                print("reached end of recipe")
                time.sleep(UPDATE_RATE_S)
                break
            print(f'at {recipe[time_unit.column_name][soonest_row]} {time_unit.keyword}s elapsed')
            print(recipe.iloc[soonest_row])
            messages = [
                (f"nodered/plc/write/{plc_id}/{TAG_CUT_IN}", json.dumps(float2arr(float(recipe["Volume Cut-in (L)"][soonest_row]))), 0, True),
                (f"nodered/plc/write/{plc_id}/{TAG_CUT_OUT}", json.dumps(float2arr(float(recipe["Volume Cut-out (L)"][soonest_row]))), 0, True),
                (f"nodered/plc/write/{plc_id}/{TAG_CF}", json.dumps(float2arr(float(recipe["EC"][soonest_row]))), 0, True)
            ]
            print("publishing vals...")
            publish.multiple(messages)
            print(f"done. waiting {UPDATE_RATE_S} seconds\n")
            time.sleep(UPDATE_RATE_S)


if __name__ == "__main__":
    main()

