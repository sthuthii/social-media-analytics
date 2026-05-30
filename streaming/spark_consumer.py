#streaming/spark_consumer.py

import json
from datetime import datetime, timezone
from kafka import KafkaConsumer
import psycopg2
from psycopg2.extras import Json
from dotenv import load_dotenv
import os

load_dotenv()

KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
TOPIC = "reddit_posts"

def get_connection():
    return psycopg2.connect(
        host=os.getenv("DB_HOST", "db"),
        port=os.getenv("DB_PORT", 5432),
        dbname=os.getenv("DB_NAME", "trendpulse"),
        user=os.getenv("DB_USER", "postgres"),
        password=os.getenv("DB_PASSWORD","postgres")
    )

def setup_table(conn):
    with conn.cursor() as cur:
        cur.execute("""
                    CREATE TABLE IF NOT EXISTS raw_posts(
                    id                  SERIAL PRIMARY KEY,
                    post_id             VARCHAR(50) UNIQUE,
                    title               TEXT,
                    subreddit           VARCHAR(100),
                    upvotes             INTEGER,
                    upvote_ratio        NUMERIC(4,2),
                    num_comments        INTEGER,
                    awards              INTEGER,
                    created_minutes_ago  INTEGER,
                    upvote_velocity     NUMERIC(10, 2),
                    comment_ratio       NUMERIC(10,4),
                    author_karma        INTEGER,
                    is_self_post        BOOLEAN,
                    hour_posted         INTEGER,
                    day_of_week         INTEGER,
                    is_viral            BOOLEAN,
                    scraped_at          TIMESTAMP,
                    inserted_at         TIMESTAMP DEFAULT NOW()
                        )
                    """)
    conn.commit()
    print("[Consumer] Table ready.")

def save_post(conn, post: dict):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO raw_posts(
                    post_id, title, subreddit, upvotes, upvote_ratio, num_comments,
                    awards, created_minutes_ago, upvote_velocity, comment_ratio, author_karma, is_self_post, hour_posted,
                    day_of_week, is_viral, scraped_at)
                    VALUES (
                    %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s
                    ) ON CONFLICT (post_id) DO NOTHING
                    """, (
                        post["post_id"], post["title"], post["subreddit"],
            post["upvotes"], post["upvote_ratio"], post["num_comments"],
            post["awards"], post["created_minutes_ago"],
            post["upvote_velocity"], post["comment_ratio"],
            post["author_karma"], post["is_self_post"],
            post["hour_posted"], post["day_of_week"],
            post["is_viral"], post["scraped_at"]
                    ))
    conn.commit()

def run_consumer():
    print(f"[Consumer] Connecting to Kafka at {KAFKA_BOOTSTRAP_SERVERS}...")

    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="trendpulse-consumer",
        consumer_timeout_ms=15000   # stop after 15s of no messages
    )

    conn = get_connection()
    setup_table(conn)

    count = 0
    print("[Consumer] Listening for messages...")

    for message in consumer:
        post = message.value
        save_post(conn, post)
        count += 1
        print(f"  [Saved] {post['subreddit']} | "
              f"upvotes: {post['upvotes']} | "
              f"viral: {post['is_viral']}")

    conn.close()
    consumer.close()
    print(f"[Consumer] Done! {count} posts saved to DB.")


if __name__ == "__main__":
    run_consumer()