from kafka import KafkaProducer
from googleapiclient.discovery import build
import json
import os
from dotenv import load_dotenv

load_dotenv()

# Setup Kafka producer
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Setup YouTube API
api_key= os.getenv('YOUTUBE_API')
youtube = build('youtube', 'v3', developerKey=api_key)

def fetch_youtube_comments(query):
    search_response = youtube.search().list(
        q=query,
        part='id',
        type='video',
        maxResults=3
    ).execute()

    for item in search_response['items']:
        video_id = item['id']['videoId']
        comment_response = youtube.commentThreads().list(
            part='snippet',
            videoId=video_id,
            maxResults=5,
            textFormat='plainText'
        ).execute()

        for comment in comment_response['items']:
            text = comment['snippet']['topLevelComment']['snippet']['textDisplay']
            producer.send('youtube-comments', {'video_id': video_id, 'comment': text})
            print(f"Sent: {text}")
