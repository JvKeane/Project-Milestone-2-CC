import redis  # pip install redis
from google.cloud import pubsub_v1
import base64
import glob
import os


service_key = glob.glob("*.json")
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = service_key[0];
ip = "35.234.247.89"
project_id = "tidy-fort-449117-c9"
subscription_name = "Design2Redis-sub"
subscription_path = f"projects/{project_id}/topics/{subscription_name}"
r = redis.Redis(host=ip, port=6379, db=0, password='johvonnekeane')
count = 1
# keys = list(r.scan_iter('image:*'))

def callback(message, i = count):

    try:
        data = base64.b64decode(message.data)


        key = message.ordering_key

        if key:
            r.set(key, data)
            filename = key.replace("image:", "")
            output_path = f"received_images/{filename}"
            with open(output_path, "wb") as file:
                file.write(data)

            print(f"Downloaded {filename} to {output_path}")
            message.ack()
    except Exception as e:
        print(f"Failed to process message: {e}")
        message.nack()


subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(project_id, subscription_name)

print(f"Listening for messages on {subscription_path}...\n")


future = subscriber.subscribe(subscription_path, callback=callback)

try:
    future.result()
except KeyboardInterrupt:
    future.cancel()
    print("Subscriber stopped.")

