"""
Spring Workers — Fruit Picking Simulation
==========================================
OS Concepts demonstrated:
  - Parallel processes (multiprocessing)
  - Mutual exclusion (multiprocessing.Lock)
  - Signaling / condition synchronization (multiprocessing.Event)
  - Producer-Consumer pattern (pickers produce fruit into crate, loader consumes full crates)

Backend serves a Flask web app with Server-Sent Events (SSE) for real-time UI updates.
"""

import threading
import multiprocessing
import time
import json
import random
import queue
from datetime import datetime
from flask import Flask, render_template, jsonify, request, Response

app = Flask(__name__)

NUM_FRUITS_DEFAULT = 52
CRATE_CAPACITY_DEFAULT = 12
NUM_PICKERS = 3

# Main server state for SSE
sse_subscribers = []
sse_lock = threading.Lock()

def broadcast_worker(event_queue):
    """Background thread to read from multiprocessing queue and broadcast to SSE."""
    while True:
        try:
            msg = event_queue.get()
            if msg is None: break
            with sse_lock:
                dead = []
                for q in sse_subscribers:
                    try:
                        q.put_nowait(msg)
                    except queue.Full:
                        dead.append(q)
                for q in dead:
                    sse_subscribers.remove(q)
        except Exception:
            pass

manager = None
sim = {}
event_queue = None
broadcast_thread = None
picker_processes = []
loader_process = None

def broadcast_event(event_type, data):
    """Push an event to the queue for the broadcast thread."""
    if event_queue is not None:
        msg = {"type": event_type, "data": data, "time": datetime.now().strftime("%I:%M:%S %p")}
        event_queue.put(msg)

def broadcast_lock_status(resource, status, owner=""):
    broadcast_event("lock_status", {
        "resource": resource,
        "status": status,
        "owner": owner
    })

def add_log(message, agent="SYSTEM"):
    entry = {
        "time": datetime.now().strftime("%I:%M:%S %p"),
        "agent": agent,
        "message": message,
    }
    if "logs" in sim:
        logs = sim["logs"]
        logs.append(entry)
        sim["logs"] = logs # force manager update
    broadcast_event("log", entry)

def reset_simulation(num_fruits=NUM_FRUITS_DEFAULT, crate_capacity=CRATE_CAPACITY_DEFAULT):
    global sim, manager, event_queue, broadcast_thread
    
    if manager is None:
        manager = multiprocessing.Manager()
        event_queue = multiprocessing.Queue()
        broadcast_thread = threading.Thread(target=broadcast_worker, args=(event_queue,), daemon=True)
        broadcast_thread.start()

    # To reset safely
    global picker_processes, loader_process
    
    if 'picker_processes' in globals() and picker_processes:
        for p in picker_processes:
            if hasattr(p, 'terminate') and p.is_alive(): p.terminate()
    if 'loader_process' in globals() and loader_process and hasattr(loader_process, 'terminate') and loader_process.is_alive():
        loader_process.terminate()
        
    picker_processes = []
    loader_process = None

    sim = manager.dict()
    sim["tree"] = manager.list(range(1, num_fruits + 1))
    sim["tree_total"] = num_fruits
    sim["crate"] = manager.list()
    sim["crate_id"] = 1
    sim["crate_capacity"] = crate_capacity
    sim["truck"] = manager.list()
    
    picker_status = manager.dict()
    for i in range(1, NUM_PICKERS + 1):
        picker_status[i] = "IDLE"
    sim["picker_status"] = picker_status
    
    sim["loader_status"] = "Waiting for full slots"
    sim["running"] = False
    sim["finished"] = False
    
    sim["tree_lock"] = manager.Lock()
    sim["crate_lock"] = manager.Lock()
    sim["crate_full"] = manager.Event()
    sim["new_crate_ready"] = manager.Event()
    sim["all_done"] = manager.Event()
    
    sim["active_pickers"] = NUM_PICKERS
    sim["active_pickers_lock"] = manager.Lock()
    
    sim["logs"] = manager.list()
    sim["new_crate_ready"].set()

def picker_worker(picker_id, sim_state, event_q):
    global sim, event_queue
    sim = sim_state
    event_queue = event_q
    
    name = f"PICKER{picker_id}"
    
    ps = sim["picker_status"]
    ps[picker_id] = "ACTIVE"
    sim["picker_status"] = ps
    
    broadcast_event("picker_status", {"id": picker_id, "status": "ACTIVE"})
    add_log(f"Picker {picker_id} started working", name)

    while sim["running"]:
        fruit = None
        broadcast_lock_status("tree", "LOCKING", name)
        with sim["tree_lock"]:
            broadcast_lock_status("tree", "LOCKED", name)
            time.sleep(1.0)
            tree = sim["tree"]
            if len(tree) > 0:
                fruit = tree.pop(0)
                sim["tree"] = tree
            else:
                sim["tree"] = tree
        broadcast_lock_status("tree", "UNLOCKED", name)

        if fruit is None:
            break

        time.sleep(random.uniform(1.5, 3.5))

        broadcast_event("fruit_picked", {
            "picker": picker_id,
            "fruit": fruit,
            "remaining": len(sim["tree"]),
        })

        placed = False
        while not placed and sim["running"]:
            sim["new_crate_ready"].wait(timeout=1.0)
            if not sim["running"]:
                break

            broadcast_lock_status("crate", "LOCKING", name)
            with sim["crate_lock"]:
                crate = sim["crate"]
                if len(crate) < sim["crate_capacity"]:
                    broadcast_lock_status("crate", "LOCKED", name)
                    time.sleep(1.2)
                    slot_index = len(crate)
                    crate.append(fruit)
                    sim["crate"] = crate
                    add_log(f"Picked fruit #{fruit} and placed in crate slot {slot_index + 1}", name)

                    broadcast_event("crate_update", {
                        "picker": picker_id,
                        "fruit": fruit,
                        "slot": slot_index,
                        "crate_id": sim["crate_id"],
                        "crate": list(sim["crate"]),
                    })

                    if len(sim["crate"]) >= sim["crate_capacity"]:
                        add_log(f"Crate is full! Calling loader...", name)
                        broadcast_event("crate_full", {"picker": picker_id, "crate_id": sim["crate_id"]})
                        sim["new_crate_ready"].clear()
                        sim["crate_full"].set()
                    placed = True
                else:
                    add_log("Crate reached capacity before placement. Waiting for fresh crate...", name)
            
            broadcast_lock_status("crate", "UNLOCKED", name)

        if placed and slot_index + 1 >= sim["crate_capacity"]:
            sim["new_crate_ready"].wait(timeout=10)

    ps = sim["picker_status"]
    ps[picker_id] = "DONE"
    sim["picker_status"] = ps
    
    broadcast_event("picker_status", {"id": picker_id, "status": "DONE"})
    add_log(f"Picker {picker_id} finished — tree is bare", name)

    with sim["active_pickers_lock"]:
        sim["active_pickers"] -= 1
        remaining = sim["active_pickers"]

    if remaining == 0:
        sim["all_done"].set()
        sim["crate_full"].set()

def loader_worker(sim_state, event_q):
    global sim, event_queue
    sim = sim_state
    event_queue = event_q
    
    name = "LOADER"
    sim["loader_status"] = "Waiting for full slots"
    broadcast_event("loader_status", {"status": sim["loader_status"]})
    add_log("Loader ready and waiting", name)

    while sim["running"]:
        sim["crate_full"].wait(timeout=1)
        if not sim["crate_full"].is_set():
            continue

        sim["crate_full"].clear()

        broadcast_lock_status("crate", "LOCKING", name)
        with sim["crate_lock"]:
            broadcast_lock_status("crate", "LOCKED", name)
            time.sleep(1.5)
            crate = sim["crate"]
            if len(crate) >= sim["crate_capacity"]:
                crate_copy = list(crate)
                cid = sim["crate_id"]
                sim["loader_status"] = f"Loading crate #{cid} into truck..."
                broadcast_event("loader_status", {"status": sim["loader_status"]})
                add_log(f"Crate #{cid} full. Placing in truck...", name)

                time.sleep(1.5)

                truck = sim["truck"]
                truck.append({"crate_id": cid, "fruits": crate_copy})
                sim["truck"] = truck
                
                broadcast_event("truck_update", {
                    "crate_id": cid,
                    "fruits": crate_copy,
                    "truck": [c["crate_id"] for c in sim["truck"]],
                })
                add_log(f"Crate #{cid} loaded into truck ({len(crate_copy)} fruits)", name)

                sim["crate"] = manager.list()
                sim["crate_id"] += 1
                sim["loader_status"] = "Waiting for full slots"
                broadcast_event("loader_status", {"status": sim["loader_status"]})
                broadcast_event("new_crate", {"crate_id": sim["crate_id"]})
                add_log("Furnished a new empty crate for the pickers", name)
                broadcast_lock_status("crate", "UNLOCKED", name)

                sim["new_crate_ready"].set()

        if sim["all_done"].is_set():
            break

    with sim["crate_lock"]:
        crate = sim["crate"]
        if len(crate) > 0:
            crate_copy = list(crate)
            cid = sim["crate_id"]
            sim["loader_status"] = f"Loading final crate #{cid}..."
            broadcast_event("loader_status", {"status": sim["loader_status"]})
            add_log(f"Loading final partial crate #{cid} ({len(crate_copy)} fruits)", name)

            time.sleep(1.0)

            truck = sim["truck"]
            truck.append({"crate_id": cid, "fruits": crate_copy})
            sim["truck"] = truck
            
            broadcast_event("truck_update", {
                "crate_id": cid,
                "fruits": crate_copy,
                "truck": [c["crate_id"] for c in sim["truck"]],
            })
            add_log(f"Final crate #{cid} loaded into truck", name)
            sim["crate"] = manager.list()
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

@app.route("/")
def index():
    return render_template("index.html")

@app.route("/start", methods=["POST"])
def start_simulation():
    data = request.get_json(silent=True) or {}
    num_fruits = int(data.get("num_fruits", NUM_FRUITS_DEFAULT))
    crate_capacity = int(data.get("crate_capacity", CRATE_CAPACITY_DEFAULT))

    if sim.get("running"):
        return jsonify({"error": "Simulation already running"}), 400

    reset_simulation(num_fruits, crate_capacity)
    sim["running"] = True

    global picker_processes, loader_process
    
    picker_processes = []
    for i in range(1, NUM_PICKERS + 1):
        p = multiprocessing.Process(target=picker_worker, args=(i, sim, event_queue))
        picker_processes.append(p)

    loader_process = multiprocessing.Process(target=loader_worker, args=(sim, event_queue))

    add_log(f"Simulation started with {num_fruits} fruits on the tree", "SYSTEM")
    broadcast_event("simulation_start", {"num_fruits": num_fruits})

    loader_process.start()
    for p in picker_processes:
        p.start()

    return jsonify({"status": "started", "num_fruits": num_fruits})

@app.route("/stop", methods=["POST"])
def stop_simulation():
    sim["running"] = False
    sim["all_done"].set()
    sim["crate_full"].set()
    sim["new_crate_ready"].set()
    add_log("Simulation stopped by user", "SYSTEM")
    return jsonify({"status": "stopped"})

@app.route("/reset", methods=["POST"])
def reset():
    reset_simulation()
    return jsonify({"status": "reset"})

@app.route("/state", methods=["GET"])
def get_state():
    return jsonify({
        "tree": list(sim.get("tree", [])),
        "tree_total": sim.get("tree_total", 0),
        "crate": list(sim.get("crate", [])),
        "crate_id": sim.get("crate_id", 1),
        "truck": list(sim.get("truck", [])),
        "picker_status": dict(sim.get("picker_status", {})),
        "loader_status": sim.get("loader_status", ""),
        "running": sim.get("running", False),
        "finished": sim.get("finished", False),
        "logs": list(sim.get("logs", [])),
    })

@app.route("/stream")
def stream():
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
                    yield ": keepalive\n\n"
        except GeneratorExit:
            with sse_lock:
                if q in sse_subscribers:
                    sse_subscribers.remove(q)

    return Response(event_stream(), mimetype="text/event-stream",
                    headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})

if __name__ == "__main__":
    reset_simulation()
    app.run(debug=False, threaded=True, port=5000)
