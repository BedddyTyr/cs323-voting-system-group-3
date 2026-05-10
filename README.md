# Distributed Voting System — CS323 Lab (Render Edition)

> **GCP → Render adaptation.**
> All GCP services are replaced with Render equivalents while preserving the
> same distributed architecture required by the lab specification.

---

## Architecture Overview

```
Edge Nodes (local Python scripts)
    │
    │  HTTP POST /vote
    ▼
Render Web Service — voting-api  (Flask)
    │
    │  RPUSH  →  Redis List "vote_queue"
    ▼
Render Redis  (message queue / Pub/Sub equivalent)
    │
    │  BLPOP  ←  polling
    ▼
Render Background Worker — voting-worker  (Python)
    │
    │  INSERT … ON CONFLICT DO UPDATE
    ▼
Render PostgreSQL — voting-db  (persistent storage / Firestore equivalent)
```

### Service Mapping (GCP → Render)

| GCP Service | Render Equivalent | Role |
|---|---|---|
| Cloud Run (API) | Web Service (`voting-api`) | Receive votes from edge nodes |
| Pub/Sub Topic + Subscription | Redis List (`vote_queue`) | Async message buffer |
| Cloud Run (Worker) | Background Worker (`voting-worker`) | Process & persist votes |
| Firestore | PostgreSQL (`voting-db`) | Persistent vote storage |

---

## Repository Structure

```
voting-system/
├── render.yaml            ← Render Blueprint (deploys everything)
├── api/
│   ├── app.py             ← Flask ingestion API
│   └── requirements.txt
├── worker/
│   ├── worker.py          ← Redis consumer + PostgreSQL writer
│   └── requirements.txt
└── edge_node/
    ├── edge_node.py       ← Local edge node script (run by each member)
    └── requirements.txt
```

---

## Setup & Deployment Instructions

### Prerequisites
- A **GitHub account** (Render deploys from GitHub)
- A **Render account** — sign up free at https://render.com
- **Python 3.11+** installed locally for edge nodes

---

### Step 1 — Push the repository to GitHub

1. Create a new **public** GitHub repository named `cs323-voting-system-groupX`
   (replace X with your group number).
2. Copy all project files into it, preserving the folder structure above.
3. Commit and push:
   ```bash
   git init
   git add .
   git commit -m "initial commit"
   git branch -M main
   git remote add origin https://github.com/YOUR_USERNAME/cs323-voting-system-groupX.git
   git push -u origin main
   ```

---

### Step 2 — Deploy with Render Blueprint (one click)

1. Go to https://dashboard.render.com
2. Click **New → Blueprint**
3. Connect your GitHub repository
4. Render will detect `render.yaml` and show four services:
   - `voting-api` (Web Service)
   - `voting-worker` (Background Worker)
   - `voting-redis` (Redis)
   - `voting-db` (PostgreSQL)
5. Click **Apply** — Render provisions and deploys everything automatically.

Wait ~3–5 minutes for all services to reach **Live** / **Available** status.

---

### Step 3 — Note your API URL

After deployment, click on `voting-api` in the Render dashboard.
Your public URL will look like:

```
https://voting-api-xxxx.onrender.com
```

Copy this — all edge nodes need it.

---

### Step 4 — Run Edge Nodes (each group member)

Install dependencies once:
```bash
cd edge_node
pip install -r requirements.txt
```

Each group member runs with their **own NODE_ID**:

| Member | Command |
|---|---|
| Member 1 | `NODE_ID=node_1 API_URL=https://voting-api-xxxx.onrender.com python edge_node.py` |
| Member 2 | `NODE_ID=node_2 API_URL=https://voting-api-xxxx.onrender.com python edge_node.py` |
| Member 3 | `NODE_ID=node_3 API_URL=https://voting-api-xxxx.onrender.com python edge_node.py` |
| Member 4 | `NODE_ID=node_4 API_URL=https://voting-api-xxxx.onrender.com python edge_node.py` |
| Member 5 | `NODE_ID=node_5 API_URL=https://voting-api-xxxx.onrender.com python edge_node.py` |

On **Windows CMD** use:
```cmd
set NODE_ID=node_1 && set API_URL=https://voting-api-xxxx.onrender.com && python edge_node.py
```

On **Windows PowerShell**:
```powershell
$env:NODE_ID="node_1"; $env:API_URL="https://voting-api-xxxx.onrender.com"; python edge_node.py
```

---

### Step 5 — Verify the System

**Check the queue size (Pub/Sub equivalent):**
```
GET https://voting-api-xxxx.onrender.com/queue/size
```

**Check the database:**  
Go to **Render Dashboard → voting-db → Connect** and use the PSQL command
provided, then run:
```sql
SELECT choice, COUNT(*) FROM votes GROUP BY choice;
SELECT node_id, COUNT(*) FROM votes GROUP BY node_id;
SELECT * FROM votes ORDER BY processed_at DESC LIMIT 10;
```

**Check worker logs:**  
Render Dashboard → `voting-worker` → Logs

---

## Fault Injection & Testing (Part 5)

### Simulating Message Duplication

In `edge_node/edge_node.py`, find the `run_edge_node` function and uncomment:

```python
# send_vote(vote, duplicate=True)
```

This sends each vote twice, simulating retry-induced duplication.  
Thanks to the `ON CONFLICT DO UPDATE` in the worker, Firestore/PostgreSQL will
**not** create duplicate rows — demonstrating idempotency.

---

### Simulating Worker Failure

1. Go to **Render Dashboard → voting-worker**
2. Click **Suspend Service** (top-right menu)
3. Keep all edge nodes running — observe:
   - API continues accepting votes ✓
   - Redis queue grows (check `/queue/size`) ✓
   - PostgreSQL stops receiving new rows ✓
   - No crash, no data loss ✓

---

### Simulating Worker Recovery

1. Go to **Render Dashboard → voting-worker**
2. Click **Resume Service**
3. Observe in worker logs:
   - Queued messages are processed in batches ✓
   - PostgreSQL rows resume updating ✓
   - No manual intervention needed ✓

---

## Performance Analysis (Part 6)

### Measuring End-to-End Latency

The `time_created` field is set at the edge node.  
The `processed_at` field is set when the worker writes to PostgreSQL.

```sql
SELECT
    user_id,
    node_id,
    processed_at - timestamp AS latency_seconds
FROM votes
ORDER BY processed_at DESC
LIMIT 20;
```

### Measuring Throughput

```sql
-- Votes per node
SELECT node_id, COUNT(*) AS vote_count FROM votes GROUP BY node_id;

-- Votes per minute
SELECT
    DATE_TRUNC('minute', TO_TIMESTAMP(processed_at)) AS minute,
    COUNT(*) AS votes
FROM votes
GROUP BY 1
ORDER BY 1;
```

---

## Individual Reflections

*(Each group member adds their reflection here — see lab spec Section 7)*

**[Member 1 - Node 1]:**
> ...

**[Member 2 - Node 2]:**
> ...

**[Member 3 - Node 3]:**
> ...

**[Member 4 - Node 4]:**
> ...

**[Member 5 - Node 5]:**
> ...

---

## Deployed API Endpoint

```
https://voting-api-xxxx.onrender.com
```
*(Replace with your actual Render URL after deployment)*
