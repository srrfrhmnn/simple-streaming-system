# producer.py

import time
import cv2

from kafka import SimpleProducer, KafkaClient

# connecting to kafka server
kafka = KafkaClient("localhost:9092")
producer = SimpleProducer(kafka)

# assign a topic

topic = "topic" 

def video_emitter(video):
    
    # opening the video
    video = cv2.VideoCapture(video)

    print("emitting...")

    # reading the file
    while (video.isOpened):

        # read the image in each frame
        success, image = video.read()

        # check if the file has read to the end
        if not success:
            print("err")
            break
        
        # convert the image to png 
        ret, jpeg = cv2.imencode(".png", image)

        # convert the image to bytes and send to kafka
        producer.send_messages(topic, jpeg.tobytes())

        # create sleep time of 0.2s to reduce CPU usage
        time.sleep(0.2)

    # clear the capture
    video.release()
    print("done emitting.")

if __name__ == "__main__":
    video_emitter("video.mp4")