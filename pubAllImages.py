from google.cloud import pubsub_v1  # pip install google-cloud-pubsub  ##to install
import glob  # for searching for json file
import base64
import os

# Search the current directory for the JSON file (including the service account key)
# to set the GOOGLE_APPLICATION_CREDENTIALS environment variable.
service_key = glob.glob("*.json")
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = service_key[0];

# Set the project_id with your project ID
project_id = "tidy-fort-449117-c9";
topic_name = "Design2Redis";  # change it for your topic name if needed

# create a publisher and get the topic path for the publisher
publisher_options = pubsub_v1.types.PublisherOptions(enable_message_ordering=True)
publisher = pubsub_v1.PublisherClient(publisher_options=publisher_options)
topic_path = publisher.topic_path(project_id, topic_name)
print(f"Published messages with ordering keys to {topic_path}.")

images = glob.glob("images/*.png")
total_images = len(images)

for i, image in enumerate(images):

    with open(image, "rb") as f:
        value = base64.b64encode(f.read());

    key = f"image:{os.path.basename(image)}"

    try:
        future = publisher.publish(topic_path, value, ordering_key=key);
        future.result()
        print(f"Published image {i+1} of {total_images} successfully")
    except:
        print(f"Failed to publish image {i+1} of {total_images}...")
