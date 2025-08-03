import streamlit as st
import threading
import time
import json
from kafka import KafkaConsumer
from producer import fetch_youtube_comments
from transformers import pipeline

# Title
st.title("🔍 YouTube Comment Streamer + Sentiment")

# Text input
query = st.text_input("Enter a topic (e.g. iPhone, AI, etc.):")

# Load Hugging Face sentiment pipeline
@st.cache_resource
def load_sentiment_model():
    return pipeline("sentiment-analysis", model="distilbert-base-uncased-finetuned-sst-2-english")

sentiment_pipeline = load_sentiment_model()

# Session state
if 'comments' not in st.session_state:
    st.session_state.comments = []

# Start Kafka Producer
def start_producer(q):
    fetch_youtube_comments(q)

if query:
    st.success(f"Fetching and streaming comments for: {query}")

    if st.button("Clear Comments"):
        st.session_state.comments = []

    threading.Thread(target=start_producer, args=(query,), daemon=True).start()
    time.sleep(2)

    st.subheader("📥 All Comments")

    # Kafka Consumer
    consumer = KafkaConsumer(
        'youtube-comments',
        bootstrap_servers='localhost:9092',
        auto_offset_reset='earliest',
        group_id='streamlit-group',
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        consumer_timeout_ms=5000
    )

    # Fetch and process messages
    new_comments = []
    for msg in consumer:
        comment_data = msg.value
        comment_text = comment_data.get("comment", "")
        video_id = comment_data.get("video_id", "")

        # Sentiment analysis (limit input to 512 tokens)
        try:
            result = sentiment_pipeline(comment_text[:512])[0]
            label = result['label']  # 'POSITIVE' or 'NEGATIVE'
            score = result['score']
            emoji = "😊" if label == "POSITIVE" else "😠"
            sentiment = f"{emoji} {label.capitalize()} ({score:.2f})"
        except Exception as e:
            sentiment = "❓ Unknown"

        new_comments.append({
            'video_id': video_id,
            'comment': comment_text,
            'sentiment': sentiment,
            'timestamp': time.strftime("%H:%M:%S")
        })

    st.session_state.comments.extend(new_comments)

    # Display comments
    with st.container():
        if st.session_state.comments:
            st.write(f"**Total Comments: {len(st.session_state.comments)}**")
            for i, comment_data in enumerate(reversed(st.session_state.comments)):
                with st.expander(f"Comment {len(st.session_state.comments) - i} - {comment_data['timestamp']}", expanded=False):
                    st.markdown(f"**🎥 Video ID**: {comment_data['video_id']}")
                    st.markdown(f"💬 {comment_data['comment']}")
                    st.markdown(f"📊 Sentiment: {comment_data['sentiment']}")
        else:
            st.info("No comments received yet. Please wait...")

    time.sleep(3)
    st.rerun()
