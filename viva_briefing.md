# Viva Briefing: Spring Workers (Multiprocessing Implementation)

## 1. Important Definitions & OS Concepts
*   **Process:** An independent program in execution with its own isolated memory space. Unlike threads, processes do not share variables by default.
*   **Concurrency vs. Parallelism:** Concurrency is interleaving tasks. Parallelism (what we achieve here using `multiprocessing`) is physically executing tasks simultaneously on multiple CPU cores.
*   **Inter-Process Communication (IPC):** Mechanisms that allow separate processes to share data. Here, we use `multiprocessing.Manager()` to create shared objects (lists, dicts) across our isolated worker processes.
*   **Mutual Exclusion (Mutex / Lock):** A sync mechanism (`manager.Lock()`) ensuring only one process accesses a shared resource (the tree or the crate) at any exact moment.
*   **Critical Section:** The specific block of code where shared memory is accessed and modified. It must be wrapped in a lock.
*   **Event (Condition Synchronization):** A flag (`manager.Event()`) used for process-to-process signaling. One process waits (`wait()`) for the flag to become true, and another sets it (`set()`).
*   **Producer-Consumer Problem:** A classic OS synchronization dilemma. Pickers (Producers) add fruit to a bounded buffer (Crate). The Loader (Consumer) empties the buffer when full.

---

## 2. Project Mechanisms & Logic Flow

### How State is Shared (The Manager & The Queue)
Since OS processes do not share memory, if a Picker modifies a standard Python dictionary, the Flask app won't see it. 
*   **`multiprocessing.Manager()`:** We use this to wrap our state (`sim`). It runs a hidden server process that holds the real data, and gives "proxies" to the Pickers, Loader, and Flask. When a Picker locks the crate and appends a fruit, the Manager safely updates the real list.
*   **`multiprocessing.Queue()`:** Pickers use this to send log messages and state updates back to the Flask process. Flask runs a dedicated `broadcast_worker` thread to continuously read this queue and push Server-Sent Events (SSE) to the frontend UI.

### Function Breakdown
1.  **`picker_worker(picker_id, sim_state, event_q)` (The Producer):**
    *   Runs in an infinite loop while `sim["running"]` is true.
    *   **Tree Phase:** Acquires `tree_lock`, pops fruit #1, releases `tree_lock`.
    *   **Crate Phase:** Waits for `new_crate_ready`. Acquires `crate_lock`. Appends fruit.
    *   **Signaling:** Checks if `len(crate) == capacity`. If true, it clears the `new_crate_ready` event (pausing other pickers) and triggers `crate_full.set()` (waking the loader). Releases `crate_lock`. Stops when tree is bare.
2.  **`loader_worker(sim_state, event_q)` (The Consumer):**
    *   Sleeps via `crate_full.wait()`.
    *   When awakened, acquires `crate_lock`, copies fruits to the "truck", clears the crate, assigns a new `crate_id`.
    *   Sets `new_crate_ready.set()` to unfreeze waiting pickers.
    *   When pickers are finished, it intercepts the `all_done` event, loads the final partial crate, and terminates.
3.  **`reset_simulation()`:** Kills old dangling processes. Rebuilds the `Manager`, resets the Tree list, wipes the Truck, and re-initializes all Locks and Events.

---

## 3. Worked Example / Test Case
**Scenario:** 3 Pickers. Tree has 5 fruits `[1, 2, 3, 4, 5]`. Crate capacity is 3.
1.  **Start:** Loader goes to sleep (`crate_full.wait()`). Pickers begin.
2.  **T=1:** Picker 1 gets `tree_lock`, pops Fruit 1. Picker 2 and 3 are blocked waiting for `tree_lock`.
3.  **T=2:** Picker 2 gets `tree_lock`, pops Fruit 2. Picker 1 gets `crate_lock`, puts Fruit 1 in Crate. Crate is now `[1]`.
4.  **T=3:** Picker 3 pops Fruit 3. Picker 2 puts Fruit 2 in Crate. Crate is `[1, 2]`. 
5.  **T=4:** Picker 3 puts Fruit 3 in Crate. Crate is `[1, 2, 3]`. Crate is now full (capacity 3).
6.  **Signaling (Crucial Step):** Picker 3 clears `new_crate_ready` and sets `crate_full`. Picker 3 unlocks crate.
7.  **Loader Wakes:** Loader was waiting for `crate_full`. It wakes, locks crate, moves `[1, 2, 3]` to the Truck. It empties the crate to `[]`. It sets `new_crate_ready` to wake Pickers 1 and 2 (who might be holding fruits 4 and 5).
8.  **Ending:** Pickers grab fruits 4 and 5. Tree is empty (`[]`). Pickers break their loops. Last active picker signals `all_done`. Loader wakes, grabs the partial crate `[4, 5]`, moves it to truck, and exits.

---

## 4. Corner Questions & Viva Deep-Dives

**Q1: Why did we transition from `threading` to `multiprocessing`?**
*Answer:* The assignment prompt explicitly states: *"Launch three 'picker' processes and a 'loader' process"*. In Python, multithreading relies on the Global Interpreter Lock (GIL) and shares memory implicitly. To accurately simulate true OS-level processes executing in parallel with separate memory footprints, `multiprocessing` is required. Use of `Manager()` and Inter-Process Communication validates this OS concept.

**Q2: What happens if two pickers try to add a fruit to a crate that only has 1 empty slot left?**
*Answer:* `crate_lock` prevents race conditions. If Picker 1 has the lock, Picker 2 is frozen. Picker 1 fills the crate, signals the loader, and clears the `new_crate_ready` event. When Picker 2 finally gets the lock, Picker 2's logic checks `if len(crate) < capacity`. Seeing it is full, Picker 2 bypasses placement, unlocks the crate, and loops back to `new_crate_ready.wait()` until the loader provides an empty crate.

**Q3: How do you prevent deadlocks in this project?**
*Answer:* Deadlocks occur when resources hold locks and wait on each other. We prevent this by enforcing a strict locking hierarchy and minimizing lock time. A picker never holds the `tree_lock` while waiting for the `crate_lock`. It acquires the tree, drops it, then independently acquires the crate.

**Q4: Why can't the Pickers just use a standard Python `[]` list for the crate or the tree?**
*Answer:* Because Pickers are separate OS processes. Standard variable lists are duplicated across processes; modifying one doesn't modify the other. We must use `manager.list()` which stores the list in a central server process, allowing true shared-memory modifications over IPC.

**Q5: What is the `multiprocessing.Queue()` used for in the code?**
*Answer:* Flask handles the web interface and Server-Sent Events (SSE) on the main process. Since the Pickers and Loader run in separate, isolated processes, they cannot push network events directly to Flask's variables. We use `event_queue` for IPC: workers push dictionaries (like logs or fruit updates) into the queue, and Flask continuously reads the queue to broadcast to the browser.

**Q6: What happens if the tree is completely bare (0 fruits) at the very start?**
*Answer:* The pickers enter the loop, acquire the `tree_lock`, check `if len(tree) > 0`, realize it's false, and immediately break without picking anything. The active picker count drops to 0, triggering `all_done`, which wakes the loader to shut down immediately without doing anything.

**Q7: How is the final, partially filled crate handled?**
*Answer:* Near the end of the `picker_worker`, we track active pickers. When `active_pickers == 0`, we call `sim["all_done"].set()`. The Loader's loop checks for this flag. Once seen, it breaks its main loop, does one final lock of the crate, and if `len(crate) > 0`, loads it into the truck before exiting.