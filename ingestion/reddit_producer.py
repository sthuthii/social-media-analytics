import json
import time
import random
from datetime import datetime, timezone
from kafka import KafkaProducer
import os
from dotenv import load_dotenv


load_dotenv()

KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
TOPIC = "reddit_posts"

SUBREDDITS = [
    "technology", "python", "programming", "datascience", "MachineLearning",
    "webdev", "entrepreneur", "bigdata", "deeplearning"
]

TITLES = [
    "I built a tool that {action} and got {number}k upvotes",
    "Why {topic} is the future of {field}",
    "Just released my open source {tool} — feedback welcome",
    "Hot take: {topic} is overrated and here's why",
    "Ask HN: How do you {action} at scale?",
    "Show Reddit: I spent 6 months building {tool}",
    "The {field} industry is about to be disrupted by {topic}",
    "TIL that {topic} can {action} in under 10 minutes",
    "Finally quit my job to work on {tool} full time",
    "Why I switched from {field} to {topic} and never looked back",
]

ACTIONS   = ["automates deployments", "tracks expenses", "monitors APIs",
             "predicts churn", "scrapes data", "analyses sentiment"]
TOPICS    = ["AI", "Rust", "Web3", "Kafka", "dbt", "Airflow",
             "LLMs", "edge computing", "vector databases"]
FIELDS    = ["software engineering", "data science", "DevOps",
             "product management", "machine learning"]
TOOLS     = ["CLI tool", "VS Code extension", "Python library",
             "dashboard", "API wrapper", "data pipeline"]

def generate_title() -> str:
    template = random.choice(TITLES)
    return template.format(
        action=random.choice(ACTIONS),
        number=random.randint(1,50),
        topic=random.choice(TOPICS),
        field=random.choice(FIELDS),
        tool=random.choice(TOOLS)
    )

def generate_mock_post()->dict:
    """Generate a realistic fake Reddit post"""
    created_minutes_ago = random.randint(1,120)
    upvotes = random.randint(0,3000)
    comments = random.randint(0,int(upvotes*0.3)+1)
    awards = random.randint(0,10)

    is_viral = upvotes > 1000 and created_minutes_ago < 60

    return {
        "post_id":          f"mock_{random.randint(100000, 999999)}",
        "title":            generate_title(),
        "subreddit":        random.choice(SUBREDDITS),
        "upvotes":          upvotes,
        "upvote_ratio":     round(random.uniform(0.5, 1.0), 2),
        "num_comments":     comments,
        "awards":           awards,
        "created_minutes_ago": created_minutes_ago,
        "upvote_velocity":  round(upvotes / max(created_minutes_ago, 1), 2),
        "comment_ratio":    round(comments / max(upvotes, 1), 4),
        "author_karma":     random.randint(100, 500000),
        "is_self_post":     random.choice([True, False]),
        "hour_posted":      random.randint(0, 23),
        "day_of_week":      random.randint(0, 6),
        "scraped_at":       datetime.now(timezone.utc).isoformat(),
        "is_viral":         is_viral          # ground truth label for ML
    }


def run_producer(num_posts: int = 50, delay: float = 0.5):
    print(f"[Producer] Connecting to Kafka at {KAFKA_BOOTSTRAP_SERVERS}...")

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        retries=5
    )

    print(f"[Producer] Starting to produce {num_posts} Reddit posts to topic '{TOPIC}'...")

    for i in range(num_posts):
        post = generate_mock_post()
        producer.send(TOPIC, value=post)
        print(f"  [{i+1}/{num_posts}] {post['subreddit']} | "
              f"upvotes: {post['upvotes']} | "
              f"viral: {post['is_viral']} | "
              f"{post['title'][:50]}...")
        time.sleep(delay)

    producer.flush()
    producer.close()
    print(f"[Producer] Done! {num_posts} posts sent to Kafka.")

if __name__=="__main__":
    run_producer()