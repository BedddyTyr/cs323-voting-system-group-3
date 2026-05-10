"""
Edge Node - Simulates a distributed client device sending votes to the cloud API.
Each group member runs this script independently with a unique NODE_ID.

Usage:
    pip install requests
    NODE_ID=node_1 API_URL=https://your-api.onrender.com python edge_node.py
"""

import uuid
import random
import time
import os
import requests

# --- Configuration ---
# Each group member sets their own NODE_ID (node_1 through node_5)
NODE_ID = os.environ.get("NODE_ID", "node_1")
API_URL = os.environ.get("API_URL", "http://localhost:5000")

VOTE_ENDPOINT = f"{API_URL}/vote"
CHOICES = ["A", "B", "C"]
POLL_ID = "poll_1"

# Retry settings
MAX_RETRIES = 3
RETRY_BACKOFF = 2  # seconds between retries (doubles each attempt)

# Counters (for debugging / performance analysis)
votes_sent = 0
votes_failed = 0


def generate_vote() -> dict:
    """
    Generate a single synthetic vote with a unique user ID.
    The node_id field identifies which edge node produced this vote.
    """
    return {
        "user_id": str(uuid.uuid4()),
        "poll_id": POLL_ID,
        "choice": random.choice(CHOICES),
        "timestamp": time.time(),
        "node_id": NODE_ID,
        "time_created": time.time(),
    }


def send_vote(vote: dict, duplicate: bool = False) -> bool:
    """
    Send a vote to the Cloud API with exponential backoff retry logic.
    Returns True if the vote was successfully sent, False otherwise.

    Set duplicate=True during fault injection testing to intentionally
    send the same vote payload twice.
    """
    global votes_sent, votes_failed

    attempts = MAX_RETRIES if not duplicate else 1
    delay = RETRY_BACKOFF

    for attempt in range(1, attempts + 1):
        try:
            response = requests.post(
                VOTE_ENDPOINT,
                json=vote,
                timeout=5
            )
            if response.status_code == 200:
                votes_sent += 1
                label = "[DUPLICATE]" if duplicate else ""
                print(
                    f"[{NODE_ID}] {label} Vote sent: {vote['user_id'][:8]}... "
                    f"| Choice: {vote['choice']} "
                    f"| Total sent: {votes_sent}"
                )
                return True
            else:
                print(
                    f"[{NODE_ID}] API error {response.status_code} "
                    f"(attempt {attempt}/{attempts})"
                )
        except requests.exceptions.ConnectionError:
            print(
                f"[{NODE_ID}] Connection failed (attempt {attempt}/{attempts}) "
                f"- retrying in {delay}s..."
            )
        except requests.exceptions.Timeout:
            print(f"[{NODE_ID}] Request timed out (attempt {attempt}/{attempts})")
        except Exception as e:
            print(f"[{NODE_ID}] Unexpected error: {e}")

        if attempt < attempts:
            time.sleep(delay)
            delay *= 2  # exponential backoff

    votes_failed += 1
    print(f"[{NODE_ID}] Vote DROPPED after {attempts} attempts | Failed total: {votes_failed}")
    return False


def run_edge_node(fault_inject_duplicates: bool = False):
    """
    Main loop: continuously generate and send votes with random delays.

    Set fault_inject_duplicates=True to simulate Part 5 (fault injection),
    where the same vote is intentionally sent multiple times.
    """
    print(f"[{NODE_ID}] Edge node starting... API target: {VOTE_ENDPOINT}")
    print(f"[{NODE_ID}] Fault injection (duplicates): {fault_inject_duplicates}\n")

    while True:
        vote = generate_vote()
        send_vote(vote)

        # --- FAULT INJECTION: Duplicate transmission ---
        # Uncomment for Part 5 (simulating message duplication):
        # send_vote(vote, duplicate=True)

        # Random delay simulates unpredictable real-world user activity
        delay = random.uniform(1, 3)
        time.sleep(delay)


if __name__ == "__main__":
    # Set fault_inject_duplicates=True when doing Part 5 testing
    run_edge_node(fault_inject_duplicates=False)
