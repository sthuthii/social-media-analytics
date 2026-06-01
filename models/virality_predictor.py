# models/virality_predictor.py

import os
import json
import pandas as pd
import psycopg2
from dotenv import load_dotenv
from sklearn.ensemble import RandomForestClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import train_test_split, cross_val_score
from sklearn.metrics import classification_report, confusion_matrix
from sklearn.preprocessing import LabelEncoder
import pickle

load_dotenv()


def get_connection():
    return psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=os.getenv("DB_PORT", 5433),
        dbname=os.getenv("DB_NAME", "trendpulse"),
        user=os.getenv("DB_USER", "postgres"),
        password=os.getenv("DB_PASSWORD", "postgres")
    )


def load_features() -> pd.DataFrame:
    """Load ML-ready features from dbt mart."""
    conn = get_connection()
    query = """
        SELECT
            upvotes,
            upvote_velocity,
            comment_ratio,
            upvote_ratio,
            awards,
            author_karma,
            created_minutes_ago,
            hour_posted,
            day_of_week,
            is_weekend,
            is_self_post,
            time_of_day,
            engagement_tier,
            subreddit,
            is_viral
        FROM analytics.fct_viral_signals
    """
    df = pd.read_sql(query, conn)
    conn.close()
    print(f"[ML] Loaded {len(df)} rows from fct_viral_signals")
    return df


def engineer_features(df: pd.DataFrame) -> tuple:
    """Encode categorical features and split X, y."""

    # Encode categoricals
    le_time    = LabelEncoder()
    le_tier    = LabelEncoder()
    le_sub     = LabelEncoder()

    df["time_of_day_enc"]    = le_time.fit_transform(df["time_of_day"])
    df["engagement_tier_enc"] = le_tier.fit_transform(df["engagement_tier"])
    df["subreddit_enc"]      = le_sub.fit_transform(df["subreddit"])

    feature_cols = [
        "upvotes",
        "upvote_velocity",
        "comment_ratio",
        "upvote_ratio",
        "awards",
        "author_karma",
        "created_minutes_ago",
        "hour_posted",
        "day_of_week",
        "is_weekend",
        "is_self_post",
        "time_of_day_enc",
        "engagement_tier_enc",
        "subreddit_enc"
    ]

    X = df[feature_cols].fillna(0)
    y = df["is_viral"].astype(int)

    print(f"[ML] Features: {len(feature_cols)} columns")
    print(f"[ML] Class distribution: {y.value_counts().to_dict()}")

    return X, y, feature_cols


def train_model(X, y):
    """Train and evaluate Random Forest classifier."""
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42, stratify=y
    )

    print(f"\n[ML] Training on {len(X_train)} samples, testing on {len(X_test)}")

    # Random Forest
    rf = RandomForestClassifier(
        n_estimators=100,
        max_depth=10,
        random_state=42,
        class_weight="balanced"
    )
    rf.fit(X_train, y_train)

    y_pred = rf.predict(X_test)
    y_prob = rf.predict_proba(X_test)[:, 1]

    print("\n[ML] Classification Report:")
    print(classification_report(y_test, y_pred,
          target_names=["Not Viral", "Viral"]))

    # Cross validation
    cv_scores = cross_val_score(rf, X, y, cv=5, scoring="f1")
    print(f"[ML] Cross-val F1 scores: {cv_scores.round(3)}")
    print(f"[ML] Mean F1: {cv_scores.mean():.3f} ± {cv_scores.std():.3f}")

    return rf


def get_feature_importance(model, feature_cols: list) -> pd.DataFrame:
    """Extract and display feature importances."""
    importance_df = pd.DataFrame({
        "feature":   feature_cols,
        "importance": model.feature_importances_
    }).sort_values("importance", ascending=False)

    print("\n[ML] Top 5 features predicting virality:")
    print(importance_df.head(5).to_string(index=False))

    return importance_df


def save_predictions(model, X, df: pd.DataFrame):
    """Score all posts and save predictions to DB."""
    df["viral_probability"] = model.predict_proba(X)[:, 1].round(4)
    df["predicted_viral"]   = model.predict(X)

    conn = get_connection()
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS viral_predictions (
                id                  SERIAL PRIMARY KEY,
                post_id             VARCHAR(50),
                viral_probability   NUMERIC(6,4),
                predicted_viral     BOOLEAN,
                predicted_at        TIMESTAMP DEFAULT NOW()
            )
        """)
        cur.execute("TRUNCATE TABLE viral_predictions")
    conn.commit()

    with conn.cursor() as cur:
        for i, row in df.iterrows():
            cur.execute("""
                INSERT INTO viral_predictions
                    (viral_probability, predicted_viral)
                VALUES (%s, %s)
            """, (
                float(row["viral_probability"]),
                bool(row["predicted_viral"])
            ))
    conn.commit()
    conn.close()

    high_prob = df[df["viral_probability"] > 0.8]
    print(f"\n[ML] {len(high_prob)} posts with >80% viral probability")
    print(f"[ML] Predictions saved to viral_predictions table")


def save_model(model, feature_cols: list):
    """Pickle the model for reuse."""
    os.makedirs("models/artifacts", exist_ok=True)
    with open("models/artifacts/virality_model.pkl", "wb") as f:
        pickle.dump({"model": model, "features": feature_cols}, f)
    print("[ML] Model saved to models/artifacts/virality_model.pkl")


def run_predictor():
    print("=" * 50)
    print("  TrendPulse — Virality Predictor")
    print("=" * 50)

    df        = load_features()
    X, y, features = engineer_features(df)
    model     = train_model(X, y)

    get_feature_importance(model, features)
    save_predictions(model, X, df)
    save_model(model, features)

    print("\n[ML] Done!")


if __name__ == "__main__":
    run_predictor()