"""
Take pictures with all of the IP cameras and:
 -- calculate greenness of image
 -- create a db row about the image
 -- store the image in the image storage directory
 -- add metadata to the image file
Script runs once and terminates on its own.
To be run periodically as desired.
"""

import os
import urllib.request

from db_credentials import db_credentials
from sql_helper_funcs import *

import mysql.connector
import numpy as np
import cv2
import piexif
import piexif.helper
from scipy.stats import skew


def get_frame_from_url(url):
    raw_snap = urllib.request.urlopen(url).read()
    stamp = get_sql_timestamp()
    raw_snap_matrix = np.asarray(bytearray(raw_snap), dtype="uint8")
    frame = cv2.imdecode(raw_snap_matrix, cv2.IMREAD_UNCHANGED)
    return frame, stamp


def calculate_greenness(image_BGR):

    LOWER_RANGE = (35, 51, 51)
    UPPER_RANGE = (79, 255, 255)

    # It's easier to threshold an image by colour when it's in
    # the HSV spectrum.
    image_HSV = cv2.cvtColor(image_BGR, cv2.COLOR_BGR2HSV)

    # A function of the number of pixels that fall within a set colour range.
    green_pixels = cv2.inRange(image_HSV, LOWER_RANGE, UPPER_RANGE)
    percent_greenness = (green_pixels == 255).sum() / green_pixels.size

    return percent_greenness


def calculate_skewness(image_BGR):
    
    image_BGR = image_BGR.flatten()
    
    b = np.zeros(256, dtype=int)
    g = np.zeros(256, dtype=int)
    r = np.zeros(256, dtype=int)

    for i in range(0, len(image_BGR), 3):
        b[image_BGR[i]] += 1
        g[image_BGR[i+1]] += 1
        r[image_BGR[i+2]] += 1

    return ( skew(b), skew(g), skew(r) )


def add_zeros(s, total_length):
    # Append '0's onto the end of a string until it is the desired length.
    return "0" * max(0, total_length - len(s)) + s


def main():

    STOR_PATH = "/mnt/md0/raid1/image_storage/"
    assert STOR_PATH.endswith("/")

    db = mysql.connector.connect(**db_credentials)
    cursor = db.cursor()

    batch_ids_and_ip_addresses = sql_select(cursor, """
        SELECT b.id, c.ip_address
        FROM cameras c
        JOIN zones z ON c.zone_id=z.id
        JOIN batches b ON z.id=b.zone_id
        WHERE b.ongoing=1;
    """)

    for batch_id, ip_address in batch_ids_and_ip_addresses:

        frame, stamp = get_frame_from_url(f"http://{ip_address}/cgi-bin/api.cgi?cmd=Snap&channel=0&rs=wuuPhkmUCeI9WG7C&user=admin&password=APleasantWalkAPleasantTalkAlong")

        greenness = calculate_greenness(frame)
        b_skew, g_skew, r_skew = calculate_skewness(frame)

        cursor.execute(f"""
            INSERT INTO images
            (batch_id, timestamp, greenness, r_skew, g_skew, b_skew)
            VALUES
            ({batch_id}, '{stamp}', {greenness:.6f}, {r_skew}, {g_skew}, {b_skew});
        """)
        # This gets the id of the new row that was just inserted
        # by this MySQL client.
        cursor.execute("SELECT LAST_INSERT_ID();")
        image_id = cursor.fetchall()[0][0]

        # File path based on db image id.
        filepath_number_str = add_zeros(str(image_id), 9)
        filepath = "{}/{}/{}.jpg".format(
            filepath_number_str[:3],
            filepath_number_str[3:6],
            filepath_number_str[6:]
        )
        filepath_full = STOR_PATH + filepath

        # Add more to the row.
        cursor.execute(f"""
            UPDATE images
            SET filepath='{filepath}'
            WHERE id={image_id};
        """)

        cursor.execute(f"""
            UPDATE batches
            SET latest_greenness={greenness}
            WHERE id={batch_id};
        """)

        db.commit()

        # Generate the file path tree if it dosen't already exist.
        os.makedirs(os.path.dirname(filepath_full), exist_ok=True)
        cv2.imwrite(filepath_full, frame)

        # Add metadata to the image file.
        metadata = f"batch: {batch_id}, time: {stamp}, greenness: {greenness}, r_skew: {r_skew}, g_skew: {g_skew}, b_skew: {b_skew}"
        exif_dict = piexif.load(filepath_full)
        exif_dict["Exif"][piexif.ExifIFD.UserComment] = piexif.helper.UserComment.dump(metadata)
        piexif.insert(piexif.dump(exif_dict), filepath_full)

if __name__ == "__main__":
    main()
