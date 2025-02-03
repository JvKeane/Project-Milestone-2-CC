from google.cloud import pubsub_v1  # pip install google-cloud-pubsub
import glob  # for searching for json file
import base64
import os
import csv
import numpy as np
import time
import json

# Search the current directory for the JSON file (including the service account key)
# to set the GOOGLE_APPLICATION_CREDENTIALS environment variable.
service_key = glob.glob("*.json")
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = service_key[0]

# Set the project_id with your project ID
project_id = "tidy-fort-449117-c9"
topic_name = "Labels2DB"  # change it for your topic name if needed

# create a publisher and get the topic path for the publisher
publisher_options = pubsub_v1.types.PublisherOptions(enable_message_ordering=True)
publisher = pubsub_v1.PublisherClient(publisher_options=publisher_options)
topic_path = publisher.topic_path(project_id, topic_name)
print(f"Published messages with ordering keys to {topic_path}.")
ID = np.random.randint(0, 10000000)

with open('Labels.csv', "r") as file:
    csv_dict = list(csv.DictReader(file))
    total_rows = len(csv_dict)
    for i, item in enumerate(csv_dict):
        # Ensure there are no leading spaces in the key names
        profile_name = f"{ID}:{item.get('Timestamp')}"

        # Gather data and ensure it is properly formatted
        data = {
            "ID": ID,
            "time": int(time.time()),
            "profile_name": profile_name,
            "timestamp": item.get("Timestamp"),
            "car1_location_x": item.get("Car1_Location_X"),
            "car1_location_y": item.get("Car1_Location_Y"),
            "car1_location_z": item.get("Car1_Location_Z"),
            "car2_location_x": item.get("Car2_Location_X"),
            "car2_location_y": item.get("Car2_Location_Y"),
            "car2_location_z": item.get("Car2_Location_Z"),
            "occluded_image_view": item.get("Occluded_Image_view"),
            "occluding_car_view": item.get("Occluding_Car_view"),
            "ground_truth_view": item.get("Ground_Truth_View"),
            "pedestrian_location_x_top_left": item.get("pedestrianLocationX_TopLeft"),
            "pedestrian_location_y_top_left": item.get("pedestrianLocationY_TopLeft"),
            "pedestrian_location_x_bottom_right": item.get("pedestrianLocationX_BottomRight"),
            "pedestrian_location_y_bottom_right": item.get("pedestrianLocationY_BottomRight")
        }

        # Convert the dictionary to JSON string and encode to utf-8
        record_value = json.dumps(data).encode('utf-8')

        ID += 1

        try:
            future = publisher.publish(topic_path, record_value)

            # Ensure that the publishing has been completed successfully
            future.result()
            print(f"The message has been published successfully")
        except Exception as e:
            print(f"Failed to publish the message: {e}")
