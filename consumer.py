import streamlit as st
import subprocess
import threading
import time
from kafka import KafkaConsumer
import json
from producer import fetch_youtube_comments

st.title("🔍 YouTube Comment Streamer")
query = st.text_input("Enter a topic (e.g. iPhone, AI, etc.):")

if 'comments' not in st.session_state:
    st.session_state.comments = []

def start_producer(q):
    fetch_youtube_comments(q)

if query:
    st.success(f"Fetching and streaming comments for: {query}")
    
    # Clear previous comments when new query is entered
    if st.button("Clear Comments"):
        st.session_state.comments = []
    
    threading.Thread(target=start_producer, args=(query,), daemon=True).start()
    time.sleep(2)
    
    st.subheader("📥 All Comments")
    
    consumer = KafkaConsumer(
        'youtube-comments',
        bootstrap_servers='localhost:9092',
        auto_offset_reset='earliest',  # Changed to 'earliest' to get all messages
        group_id='streamlit-group',
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        consumer_timeout_ms=5000  # Add timeout to prevent infinite blocking
    )
    
    # Create placeholder for displaying comments
    comment_container = st.container()
    
    # Collect all messages first
    new_comments = []
    for msg in consumer:
        comment_data = msg.value
        comment_text = comment_data.get("comment", "")
        video_id = comment_data.get("video_id", "")
        
        new_comments.append({
            'video_id': video_id,
            'comment': comment_text,
            'timestamp': time.strftime("%H:%M:%S")
        })
    
    # Add new comments to session state
    st.session_state.comments.extend(new_comments)
    
    # Display all comments
    with comment_container:
        if st.session_state.comments:
            st.write(f"**Total Comments: {len(st.session_state.comments)}**")
            
            # Display comments in reverse order (newest first)
            for i, comment_data in enumerate(reversed(st.session_state.comments)):
                with st.expander(f"Comment {len(st.session_state.comments) - i} - {comment_data['timestamp']}", expanded=False):
                    st.markdown(f"**🎥 Video ID**: {comment_data['video_id']}")
                    st.markdown(f"💬 {comment_data['comment']}")
        else:
            st.info("No comments received yet. Please wait...")
    
    time.sleep(3)
    st.rerun()