from google.cloud import pubsub_v1  # pip install google-cloud-pubsub
import glob  # for searching for JSON file
import json
import os
import csv

# Search for the JSON service account key
files = glob.glob("*.json")
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = files[0]

# Set the project_id with your project ID
project_id = "tidy-fort-449117-c9"
topic_name = "Labels2DB"

# Create a publisher and get the topic path for the publisher
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_name)
print(f"Publishing messages to {topic_path}.")

file_path = "Labels.csv"


def convert_value(value):
    try:
        if "." in value:
            return float(value)
        return int(value)
    except ValueError:
        return value


with open(file_path, mode='r') as csv_file:
    reader = csv.DictReader(csv_file)

    for row in reader:
        converted_row = {key: convert_value(value) for key, value in row.items()}

        message = json.dumps(converted_row).encode('utf-8')

        print("Publishing record:", message)

        future = publisher.publish(topic_path, message)
        future.result()

print("All records have been published.")