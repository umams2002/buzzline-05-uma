""" consumer_uma.py 

Has the following functions:
- init_db(config): Initialize the SQLite database and create the 'streamed_messages' table if it doesn't exist.
- insert_message(message, config): Insert a single processed message into the SQLite database.

Example JSON message
{
    "message": "I just shared a meme! It was amazing.",
    "author": "Charlie",
    "timestamp": "2025-01-29 14:35:20",
    "category": "humor",
    "sentiment": 0.87,
    "keyword_mentioned": "meme",
    "message_length": 42
}

"""

#####################################
# Import Modules
#####################################

import json
import sqlite3
from kafka import KafkaConsumer
from utils.utils_config import get_kafka_topic, get_kafka_broker_address, get_base_data_path
from utils.utils_logger import logger
from pathlib import Path

#####################################
# Setup SQLite Connection
#####################################

def get_db_connection():
    db_path = Path(get_base_data_path()) / "sentiment_table.db"
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS category_sentiments (
            category TEXT PRIMARY KEY,
            average_sentiment REAL,
            count INTEGER
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS streamed_messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            message TEXT,
            author TEXT,
            timestamp TEXT,
            category TEXT,
            sentiment REAL,
            keyword_mentioned TEXT,
            message_length INTEGER
        )
    """)
    conn.commit()
    return conn

#####################################
# Define Consumer Function
#####################################

def consume_messages():
    topic = get_kafka_topic()
    kafka_server = get_kafka_broker_address()
    
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=kafka_server,
        value_deserializer=lambda x: json.loads(x.decode("utf-8"))
    )
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    logger.info(f"Consumer listening on topic: {topic}")
    
    for message in consumer:
        data = message.value
        category = data.get("category", "other")
        sentiment = data.get("sentiment", 0)

        # Insert message into the streamed_messages table
        cursor.execute("""
            INSERT INTO streamed_messages (
                message, author, timestamp, category, sentiment, keyword_mentioned, message_length
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (
            data["message"],
            data["author"],
            data["timestamp"],
            category,
            sentiment,
            data["keyword_mentioned"],
            data["message_length"],
        ))

        # Update category sentiment statistics
        cursor.execute("SELECT count, average_sentiment FROM category_sentiments WHERE category = ?", (category,))
        row = cursor.fetchone()
        
        if row:
            count, total_sentiment = row[0], row[1] * row[0]
        else:
            count, total_sentiment = 0, 0
        
        count += 1
        total_sentiment += sentiment
        avg_sentiment = total_sentiment / count
        
        cursor.execute(
            "INSERT INTO category_sentiments (category, average_sentiment, count) VALUES (?, ?, ?) ON CONFLICT(category) DO UPDATE SET average_sentiment = excluded.average_sentiment, count = excluded.count",
            (category, avg_sentiment, count)
        )
        conn.commit()
        
        logger.info(f"Updated sentiment for category {category}: {avg_sentiment:.2f}")

#####################################
# Conditional Execution
#####################################

if __name__ == "__main__":
    consume_messages()