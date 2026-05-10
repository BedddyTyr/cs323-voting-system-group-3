"""
Cloud Ingestion API - Receives votes from edge nodes and queues them in Redis.

Deployed as a Render Web Service.
Equivalent to GCP Cloud Run (API layer).

Environment variables required (set in Render dashboard):
    REDIS_URL   - Render Redis internal URL (e.g. redis://red-xxx:6379)
    SECRET_KEY  - Any random string for Flask
"""

import os
import json
import time
import logging

from flask import Flask, request, jsonify
import redis

# ── Setup ──────────────────────────────────────────────────────────────────────

logging.basicConfig(level=logging.INFO, format="%(asctime)s [API] %(message)s")
log = logging.getLogger(__name__)

app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", "dev-secret")

REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379")
QUEUE_NAME = "vote_queue"

try:
    r = redis.from_url(REDIS_URL, decode_responses=True)
    r.ping()
    log.info("Connected to Redis at %s", REDIS_URL)
except Exception as e:
    log.error("Could not connect to Redis: %s", e)
    r = None


# ── Routes ─────────────────────────────────────────────────────────────────────

@app.route("/health", methods=["GET"])
def health():
    """Health check endpoint for Render."""
    redis_ok = False
    try:
        if r:
            r.ping()
            redis_ok = True
    except Exception:
        pass
    return jsonify({"status": "ok", "redis": redis_ok}), 200


@app.route("/vote", methods=["POST"])
def receive_vote():
    """
    Accepts a vote payload from an edge node.
    Validates required fields, then pushes the message onto the Redis queue.

    This endpoint is intentionally stateless - it does NOT touch PostgreSQL.
    Heavy lifting is done asynchronously by the worker service.
    """
    vote = request.get_json(silent=True)

    # ── Validation ────────────────────────────────────────────────────────────
    if not vote:
        return jsonify({"error": "Empty or non-JSON payload"}), 400

    required_fields = ["user_id", "poll_id", "choice"]
    missing = [f for f in required_fields if f not in vote]
    if missing:
        return jsonify({"error": f"Missing fields: {missing}"}), 400

    valid_choices = {"A", "B", "C"}
    if vote["choice"] not in valid_choices:
        return jsonify({"error": f"Invalid choice. Must be one of {valid_choices}"}), 400

    # ── Enqueue (Pub/Sub equivalent) ──────────────────────────────────────────
    if not r:
        return jsonify({"error": "Queue unavailable"}), 503

    try:
        # Add server-side received timestamp before queuing
        vote["received_at"] = time.time()
        r.rpush(QUEUE_NAME, json.dumps(vote))

        log.info(
            "Queued vote: user=%s poll=%s choice=%s node=%s",
            vote["user_id"][:8],
            vote.get("poll_id"),
            vote.get("choice"),
            vote.get("node_id", "unknown"),
        )
        return jsonify({"status": "accepted"}), 200

    except Exception as e:
        log.error("Failed to enqueue vote: %s", e)
        return jsonify({"error": str(e)}), 500


@app.route("/queue/size", methods=["GET"])
def queue_size():
    """Returns the current number of messages waiting in the queue."""
    if not r:
        return jsonify({"error": "Redis unavailable"}), 503
    size = r.llen(QUEUE_NAME)
    return jsonify({"queue": QUEUE_NAME, "pending_messages": size}), 200


# ── Entry point ────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
