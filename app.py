"""
Spring Workers — Fruit Picking Simulation
==========================================
OS Concepts demonstrated:
  - Parallel processes (simulated via Python threads)
  - Mutual exclusion (threading.Lock)
  - Signaling / condition synchronization (threading.Event, threading.Condition)
  - Producer-Consumer pattern (pickers produce fruit into crate, loader consumes full crates)

Backend serves a Flask web app with Server-Sent Events (SSE) for real-time UI updates.
"""

import threading
import time
import json
import random
import queue
from datetime import datetime
from flask import Flask, render_template, jsonify, request, Response

# ──────────────────────────────────────────────
# Flask App
# ──────────────────────────────────────────────
app = Flask(__name__)

# ──────────────────────────────────────────────
# Global Simulation State
# ──────────────────────────────────────────────
NUM_FRUITS_DEFAULT = 52          # Default number of fruits on the tree
CRATE_CAPACITY = 12              # Each crate has 12 slots
NUM_PICKERS = 3                  # Three picker processes

# All mutable simulation state is stored in a dict so we can reset cleanly.
sim = {}                         # Will be populated by reset_simulation()

# SSE message queue — listeners subscribe to this
sse_subscribers = []             # list of queue.Queue objects
sse_lock = threading.Lock()      # protects sse_subscribers list


def broadcast_event(event_type, data):
    """Push an event to every connected SSE client."""
    msg = {"type": event_type, "data": data, "time": datetime.now().strftime("%I:%M:%S %p")}
    with sse_lock:
        dead = []
        for q in sse_subscribers:
            try:
                q.put_nowait(msg)
            except queue.Full:
                dead.append(q)
        for q in dead:
            sse_subscribers.remove(q)


def broadcast_lock_status(resource, status, owner=""):
    """
    Broadcasts the state of a mutex (tree or crate).
    Status: "LOCKING", "LOCKED", "UNLOCKED"
    """
    broadcast_event("lock_status", {
        "resource": resource,
        "status": status,
        "owner": owner
    })


def reset_simulation(num_fruits=NUM_FRUITS_DEFAULT):
    """Initialise / reset all simulation state."""
    global sim
    sim = {
        # The tree: an array of fruit IDs (integers 1..N)
        "tree": list(range(1, num_fruits + 1)),
        "tree_total": num_fruits,

        # Current crate (list of fruit IDs, max CRATE_CAPACITY)
        "crate": [],
        "crate_id": 1,              # monotonically increasing crate number

        # Truck: list of {"crate_id": int, "fruits": [...]}
        "truck": [],

        # Picker status: "IDLE" | "ACTIVE" | "DONE"
        "picker_status": {i: "IDLE" for i in range(1, NUM_PICKERS + 1)},

        # Loader status text
        "loader_status": "Waiting for full slots",

        # Running flag
        "running": False,
        "finished": False,

        # Synchronization primitives
        "tree_lock": threading.Lock(),
        "crate_lock": threading.Lock(),
        "crate_full": threading.Event(),       # pickers signal loader
        "new_crate_ready": threading.Event(),   # loader signals pickers
        "all_done": threading.Event(),          # signal loader to wrap up
        "picker_threads": [],
        "loader_thread": None,

        # Count of active pickers still running
        "active_pickers": NUM_PICKERS,
        "active_pickers_lock": threading.Lock(),

        # Logs kept server-side for late-joining clients
        "logs": [],
    }
    # The new crate is immediately ready at start
    sim["new_crate_ready"].set()


def add_log(message, agent="SYSTEM"):
    """Append a log entry."""
    entry = {
        "time": datetime.now().strftime("%I:%M:%S %p"),
        "agent": agent,
        "message": message,
    }
    sim["logs"].append(entry)
    broadcast_event("log", entry)


# ──────────────────────────────────────────────
# Picker Thread Function
# ──────────────────────────────────────────────
def picker_worker(picker_id):
    """
    Each picker runs in its own thread.
    It repeatedly:
      1. Acquires tree_lock, takes one fruit (mutual exclusion on shared tree).
      2. Acquires crate_lock, places the fruit in the crate.
      3. If crate is full → signals the loader and waits for a new crate.
      4. Stops when the tree is empty.
    """
    name = f"PICKER{picker_id}"
    sim["picker_status"][picker_id] = "ACTIVE"
    broadcast_event("picker_status", {"id": picker_id, "status": "ACTIVE"})
    add_log(f"Picker {picker_id} started working", name)

    while sim["running"]:
        # ── Step 1: Pick a fruit from the tree (critical section) ──
        fruit = None
        broadcast_lock_status("tree", "LOCKING", name)
        with sim["tree_lock"]:
            broadcast_lock_status("tree", "LOCKED", name)
            time.sleep(1.0)  # Instructional delay to show 'LOCKED' status
            if len(sim["tree"]) > 0:
                fruit = sim["tree"].pop(0)   # Take the first available fruit
            # If tree is empty, fruit stays None → picker will exit loop
        broadcast_lock_status("tree", "UNLOCKED", name)

        if fruit is None:
            break  # Tree is bare — this picker is done

        # Small random delay to simulate picking time & make animation visible
        time.sleep(random.uniform(1.5, 3.5))

        # Broadcast that fruit was removed from tree
        broadcast_event("fruit_picked", {
            "picker": picker_id,
            "fruit": fruit,
            "remaining": len(sim["tree"]),
        })

        # ── Step 2: Place fruit in crate (critical section) ──
        broadcast_lock_status("crate", "LOCKING", name)
        with sim["crate_lock"]:
            broadcast_lock_status("crate", "LOCKED", name)
            time.sleep(1.2)  # Instructional delay to show 'LOCKED' status
            slot_index = len(sim["crate"])
            sim["crate"].append(fruit)
            add_log(f"Picked fruit #{fruit} and placed in crate slot {slot_index + 1}", name)

            broadcast_event("crate_update", {
                "picker": picker_id,
                "fruit": fruit,
                "slot": slot_index,
                "crate_id": sim["crate_id"],
                "crate": list(sim["crate"]),
            })

            # ── Step 3: If crate is full, call the loader ──
            if len(sim["crate"]) >= CRATE_CAPACITY:
                add_log(f"Crate is full! Calling loader...", name)
                broadcast_event("crate_full", {"picker": picker_id, "crate_id": sim["crate_id"]})
                sim["new_crate_ready"].clear()    # We will need a new crate
                sim["crate_full"].set()            # Wake the loader
        broadcast_lock_status("crate", "UNLOCKED", name)

        # Wait outside the lock for the loader to furnish a new crate
        if slot_index + 1 >= CRATE_CAPACITY:
            sim["new_crate_ready"].wait(timeout=10)

    # ── Picker is done ──
    sim["picker_status"][picker_id] = "DONE"
    broadcast_event("picker_status", {"id": picker_id, "status": "DONE"})
    add_log(f"Picker {picker_id} finished — tree is bare", name)

    # Decrement active picker count
    with sim["active_pickers_lock"]:
        sim["active_pickers"] -= 1
        remaining = sim["active_pickers"]

    # If this was the last picker, signal the loader to wrap up
    if remaining == 0:
        sim["all_done"].set()
        sim["crate_full"].set()   # Wake loader in case it's waiting


# ──────────────────────────────────────────────
# Loader Thread Function
# ──────────────────────────────────────────────
def loader_worker():
    """
    The loader waits for a full crate signal, moves it to the truck,
    then provides a new empty crate.  After all pickers finish,
    it loads any remaining partial crate.
    """
    name = "LOADER"
    sim["loader_status"] = "Waiting for full slots"
    broadcast_event("loader_status", {"status": sim["loader_status"]})
    add_log("Loader ready and waiting", name)

    while sim["running"]:
        # Wait until a crate is full OR all pickers are done
        sim["crate_full"].wait(timeout=1)

        if not sim["crate_full"].is_set():
            continue  # Spurious wake / timeout — keep waiting

        sim["crate_full"].clear()

        # Check if we have a full crate to move
        broadcast_lock_status("crate", "LOCKING", name)
        with sim["crate_lock"]:
            broadcast_lock_status("crate", "LOCKED", name)
            time.sleep(1.5)  # Instructional delay to show 'LOCKED' status
            if len(sim["crate"]) >= CRATE_CAPACITY:
                # ── Move full crate to truck ──
                crate_copy = list(sim["crate"])
                cid = sim["crate_id"]
                sim["loader_status"] = f"Loading crate #{cid} into truck..."
                broadcast_event("loader_status", {"status": sim["loader_status"]})
                add_log(f"Crate #{cid} full. Placing in truck...", name)

                time.sleep(1.5)  # Simulate loading time

                sim["truck"].append({"crate_id": cid, "fruits": crate_copy})
                broadcast_event("truck_update", {
                    "crate_id": cid,
                    "fruits": crate_copy,
                    "truck": [c["crate_id"] for c in sim["truck"]],
                })
                add_log(f"Crate #{cid} loaded into truck ({len(crate_copy)} fruits)", name)

                # Furnish new empty crate
                sim["crate"] = []
                sim["crate_id"] += 1
                sim["loader_status"] = "Waiting for full slots"
                broadcast_event("loader_status", {"status": sim["loader_status"]})
                broadcast_event("new_crate", {"crate_id": sim["crate_id"]})
                add_log("Furnished a new empty crate for the pickers", name)
                broadcast_lock_status("crate", "UNLOCKED", name)

                sim["new_crate_ready"].set()  # Wake waiting pickers

        # If all pickers are done and signaled us
        if sim["all_done"].is_set():
            break

    # ── Final: load any partial crate ──
    with sim["crate_lock"]:
        if len(sim["crate"]) > 0:
            crate_copy = list(sim["crate"])
            cid = sim["crate_id"]
            sim["loader_status"] = f"Loading final crate #{cid}..."
            broadcast_event("loader_status", {"status": sim["loader_status"]})
            add_log(f"Loading final partial crate #{cid} ({len(crate_copy)} fruits)", name)

            time.sleep(1.0)

            sim["truck"].append({"crate_id": cid, "fruits": crate_copy})
            broadcast_event("truck_update", {
                "crate_id": cid,
                "fruits": crate_copy,
                "truck": [c["crate_id"] for c in sim["truck"]],
            })
            add_log(f"Final crate #{cid} loaded into truck", name)
            sim["crate"] = []
            broadcast_event("new_crate", {"crate_id": cid})

    sim["loader_status"] = "All done!"
    broadcast_event("loader_status", {"status": sim["loader_status"]})
    sim["finished"] = True

    total_fruits = sum(len(c["fruits"]) for c in sim["truck"])
    add_log(f"Simulation complete! {total_fruits} fruits in {len(sim['truck'])} crates loaded.", "SYSTEM")
    broadcast_event("simulation_done", {
        "total_fruits": total_fruits,
        "total_crates": len(sim["truck"]),
    })


# ──────────────────────────────────────────────
# Flask Routes
# ──────────────────────────────────────────────
@app.route("/")
def index():
    """Serve the main HTML page."""
    return render_template("index.html")


@app.route("/start", methods=["POST"])
def start_simulation():
    """Start the simulation with optional fruit count."""
    data = request.get_json(silent=True) or {}
    num_fruits = int(data.get("num_fruits", NUM_FRUITS_DEFAULT))

    if sim.get("running"):
        return jsonify({"error": "Simulation already running"}), 400

    reset_simulation(num_fruits)
    sim["running"] = True

    # Launch 3 picker threads + 1 loader thread
    for i in range(1, NUM_PICKERS + 1):
        t = threading.Thread(target=picker_worker, args=(i,), daemon=True)
        sim["picker_threads"].append(t)

    sim["loader_thread"] = threading.Thread(target=loader_worker, daemon=True)

    add_log(f"Simulation started with {num_fruits} fruits on the tree", "SYSTEM")
    broadcast_event("simulation_start", {"num_fruits": num_fruits})

    # Start all threads
    sim["loader_thread"].start()
    for t in sim["picker_threads"]:
        t.start()

    return jsonify({"status": "started", "num_fruits": num_fruits})


@app.route("/stop", methods=["POST"])
def stop_simulation():
    """Abort the running simulation."""
    sim["running"] = False
    sim["all_done"].set()
    sim["crate_full"].set()
    sim["new_crate_ready"].set()
    add_log("Simulation stopped by user", "SYSTEM")
    return jsonify({"status": "stopped"})


@app.route("/reset", methods=["POST"])
def reset():
    """Reset simulation state."""
    reset_simulation()
    return jsonify({"status": "reset"})


@app.route("/state", methods=["GET"])
def get_state():
    """Return current simulation snapshot."""
    return jsonify({
        "tree": list(sim.get("tree", [])),
        "tree_total": sim.get("tree_total", 0),
        "crate": list(sim.get("crate", [])),
        "crate_id": sim.get("crate_id", 1),
        "truck": sim.get("truck", []),
        "picker_status": sim.get("picker_status", {}),
        "loader_status": sim.get("loader_status", ""),
        "running": sim.get("running", False),
        "finished": sim.get("finished", False),
        "logs": sim.get("logs", []),
    })


@app.route("/stream")
def stream():
    """SSE endpoint — pushes real-time events to the browser."""
    def event_stream():
        q = queue.Queue(maxsize=200)
        with sse_lock:
            sse_subscribers.append(q)
        try:
            while True:
                try:
                    msg = q.get(timeout=30)
                    yield f"data: {json.dumps(msg)}\n\n"
                except queue.Empty:
                    # Send keepalive comment
                    yield ": keepalive\n\n"
        except GeneratorExit:
            with sse_lock:
                if q in sse_subscribers:
                    sse_subscribers.remove(q)

    return Response(event_stream(), mimetype="text/event-stream",
                    headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


# ──────────────────────────────────────────────
# Entry Point
# ──────────────────────────────────────────────
reset_simulation()  # Set initial state

if __name__ == "__main__":
    # threaded=True so SSE and REST work concurrently
    app.run(debug=False, threaded=True, port=5000)
