"""
Worker Service - Consumes votes from Redis queue and persists them to PostgreSQL.

Deployed as a Render Background Worker.
Equivalent to GCP Cloud Run (Worker) + Pub/Sub subscriber.

Environment variables required (set in Render dashboard):
    REDIS_URL       - Render Redis internal URL
    DATABASE_URL    - Render PostgreSQL internal URL
"""

import os
import json
import time
import logging

import redis
import psycopg2
from psycopg2.extras import RealDictCursor

# ── Setup ──────────────────────────────────────────────────────────────────────

logging.basicConfig(level=logging.INFO, format="%(asctime)s [WORKER] %(message)s")
log = logging.getLogger(__name__)

REDIS_URL    = os.environ.get("REDIS_URL",    "redis://localhost:6379")
DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://localhost/votes")
QUEUE_NAME   = "vote_queue"
POLL_TIMEOUT = 5   # seconds to block-wait on Redis BLPOP
RETRY_SLEEP  = 5   # seconds to wait before reconnecting after a crash

# Counters
processed = 0
duplicates = 0
errors = 0


# ── Database helpers ───────────────────────────────────────────────────────────

def get_db_connection():
    """Opens and returns a new PostgreSQL connection."""
    conn = psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)
    conn.autocommit = False
    return conn


def init_db(conn):
    """
    Creates the votes table if it doesn't already exist.
    The PRIMARY KEY on (user_id, poll_id) enforces idempotency -
    duplicate vote messages result in an UPDATE rather than INSERT,
    mirroring Firestore's document.set() behaviour.
    """
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS votes (
                id          SERIAL,
                user_id     TEXT        NOT NULL,
                poll_id     TEXT        NOT NULL,
                choice      TEXT        NOT NULL,
                node_id     TEXT,
                timestamp   DOUBLE PRECISION,
                received_at DOUBLE PRECISION,
                processed_at DOUBLE PRECISION,
                PRIMARY KEY (user_id, poll_id)
            );
        """)
        conn.commit()
    log.info("Database table 'votes' ready.")


# ── Redis helpers ──────────────────────────────────────────────────────────────

def get_redis_connection():
    r = redis.from_url(REDIS_URL, decode_responses=True)
    r.ping()
    return r


# ── Vote processing ────────────────────────────────────────────────────────────

def process_vote(raw_message: str, conn) -> bool:
    """
    Decode, validate, and persist a single vote message.

    Idempotency: uses INSERT ... ON CONFLICT DO UPDATE so that if the same
    (user_id, poll_id) arrives twice, the row is simply refreshed rather than
    duplicated - matching Firestore's document.set() behaviour.

    Returns True on success, False if the message should be discarded.
    """
    global processed, duplicates, errors

    # ── Decode ────────────────────────────────────────────────────────────────
    try:
        vote = json.loads(raw_message)
    except json.JSONDecodeError as e:
        log.warning("Malformed message, discarding: %s | Error: %s", raw_message[:80], e)
        errors += 1
        return False

    required = ["user_id", "poll_id", "choice"]
    if not all(k in vote for k in required):
        log.warning("Vote missing required fields, discarding: %s", vote)
        errors += 1
        return False

    # ── Idempotent upsert ─────────────────────────────────────────────────────
    doc_id = f"{vote['user_id']}_{vote['poll_id']}"   # mirrors Firestore doc ID
    processed_at = time.time()

    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO votes (user_id, poll_id, choice, node_id, timestamp, received_at, processed_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (user_id, poll_id)
                DO UPDATE SET
                    choice       = EXCLUDED.choice,
                    node_id      = EXCLUDED.node_id,
                    processed_at = EXCLUDED.processed_at;
            """, (
                vote["user_id"],
                vote["poll_id"],
                vote["choice"],
                vote.get("node_id"),
                vote.get("timestamp"),
                vote.get("received_at"),
                processed_at,
            ))
            conn.commit()

        # Detect if this was a duplicate (processed_at differs from original)
        latency = round(processed_at - vote.get("time_created", processed_at), 4)
        processed += 1
        log.info(
            "Processed vote: user=%s | poll=%s | choice=%s | node=%s | "
            "latency=%.4fs | total=%d",
            vote["user_id"][:8], vote["poll_id"], vote["choice"],
            vote.get("node_id", "?"), latency, processed,
        )
        return True

    except Exception as e:
        conn.rollback()
        log.error("DB write failed for doc_id=%s: %s", doc_id, e)
        errors += 1
        return False


# ── Main loop ──────────────────────────────────────────────────────────────────

def run_worker():
    """
    Continuously pulls votes from the Redis queue and processes them.

    Uses BLPOP for efficient blocking reads (no busy-wait polling).
    On connection loss, waits RETRY_SLEEP seconds then reconnects -
    simulating automatic recovery after worker downtime.
    """
    log.info("Worker starting...")

    db_conn = None
    r = None

    while True:
        # ── (Re)connect to dependencies ───────────────────────────────────────
        try:
            if db_conn is None or db_conn.closed:
                log.info("Connecting to PostgreSQL...")
                db_conn = get_db_connection()
                init_db(db_conn)

            if r is None:
                log.info("Connecting to Redis...")
                r = get_redis_connection()
                log.info("Worker ready - listening on queue '%s'", QUEUE_NAME)

        except Exception as e:
            log.error("Could not connect to dependencies: %s", e)
            log.info("Retrying in %ds...", RETRY_SLEEP)
            time.sleep(RETRY_SLEEP)
            db_conn = None
            r = None
            continue

        # ── Consume one message ───────────────────────────────────────────────
        try:
            # BLPOP blocks up to POLL_TIMEOUT seconds, then returns None
            result = r.blpop(QUEUE_NAME, timeout=POLL_TIMEOUT)
            if result is None:
                # No messages right now - loop back and wait again
                continue

            _, raw_message = result
            process_vote(raw_message, db_conn)

        except redis.exceptions.ConnectionError as e:
            log.warning("Lost Redis connection: %s - reconnecting...", e)
            r = None
            time.sleep(RETRY_SLEEP)

        except Exception as e:
            log.error("Unexpected worker error: %s", e)
            time.sleep(1)


if __name__ == "__main__":
    run_worker()
