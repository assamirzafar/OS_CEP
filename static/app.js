/* ═══════════════════════════════════════════════
   Spring Workers — Frontend Application Logic
   ═══════════════════════════════════════════════
   Connects to the Flask backend via SSE (Server-Sent Events)
   and REST API to drive the animated fruit-picking simulation.
*/

// ──────────────────────────────────────────────
// Constants
// ──────────────────────────────────────────────
const CRATE_CAPACITY = 12;
const MAX_VISIBLE_FRUITS = 24;   // Show at most this many circles on the tree
const FRUIT_COLORS = ['fruit-c0', 'fruit-c1', 'fruit-c2', 'fruit-c3', 'fruit-c4', 'fruit-c5'];
const PICKER_COLORS = { 1: 'var(--coral)', 2: 'var(--teal)', 3: 'var(--purple)' };

// ──────────────────────────────────────────────
// State
// ──────────────────────────────────────────────
let eventSource = null;
let allFruits = [];       // Full tree array
let totalFruits = 0;
let crateSlots = [];      // Current crate contents
let truckCrates = [];     // Shipped crate IDs
let isRunning = false;

// ──────────────────────────────────────────────
// DOM Refs
// ──────────────────────────────────────────────
const fruitGrid = document.getElementById('fruit-grid');
const extraBadge = document.getElementById('extra-fruits');
const fruitsLeftLabel = document.getElementById('fruits-left-label');
const crateGrid = document.getElementById('crate-grid');
const crateSlotsLabel = document.getElementById('crate-slots-label');
const truckCratesEl = document.getElementById('truck-crates');
const truckLabel = document.getElementById('truck-label');
const logList = document.getElementById('log-list');
const loaderStatusEl = document.getElementById('loader-status-text');
const statShipped = document.getElementById('stat-shipped');
const statRemaining = document.getElementById('stat-remaining');
const statAgents = document.getElementById('stat-agents');
const btnStart = document.getElementById('btn-start');
const btnStop = document.getElementById('btn-stop');
const btnReset = document.getElementById('btn-reset');

// ──────────────────────────────────────────────
// Initialisation
// ──────────────────────────────────────────────
initCrateGrid();

function initCrateGrid() {
    crateGrid.innerHTML = '';
    for (let i = 0; i < CRATE_CAPACITY; i++) {
        const slot = document.createElement('div');
        slot.className = 'crate-slot';
        slot.id = `crate-slot-${i}`;
        crateGrid.appendChild(slot);
    }
}

// ──────────────────────────────────────────────
// Tree Rendering
// ──────────────────────────────────────────────
function renderTree(fruits) {
    allFruits = fruits;
    fruitGrid.innerHTML = '';
    const visible = fruits.slice(0, MAX_VISIBLE_FRUITS);
    const extra = fruits.length - visible.length;

    visible.forEach(f => {
        const dot = document.createElement('div');
        dot.className = `fruit-dot ${FRUIT_COLORS[f % FRUIT_COLORS.length]}`;
        dot.id = `fruit-${f}`;
        dot.textContent = f;
        fruitGrid.appendChild(dot);
    });

    if (extra > 0) {
        extraBadge.textContent = `+${extra}`;
        extraBadge.classList.remove('hidden');
    } else {
        extraBadge.classList.add('hidden');
    }
    updateFruitsLeft(fruits.length);
}

function removeFruitFromTree(fruitId, remaining) {
    // 1. Update local state IMMEDIATELY to prevent race conditions in refill logic
    allFruits = allFruits.filter(id => id !== fruitId);

    const dot = document.getElementById(`fruit-${fruitId}`);
    if (dot) {
        dot.classList.add('picked');
        setTimeout(() => dot.remove(), 500);
    }

    // 2. Refill check: After the animation finishes, ensure we have MAX_VISIBLE_FRUITS if available
    setTimeout(() => {
        const currentVisibleCount = fruitGrid.querySelectorAll('.fruit-dot:not(.picked)').length;
        if (currentVisibleCount < MAX_VISIBLE_FRUITS && allFruits.length > currentVisibleCount) {
            // Find IDs that are currently visible
            const existingIds = Array.from(fruitGrid.querySelectorAll('.fruit-dot:not(.picked)'))
                .map(d => parseInt(d.id.replace('fruit-', '')));

            // Add the next available fruit that isn't already visible
            const nextFruit = allFruits.find(id => !existingIds.includes(id));
            if (nextFruit) {
                const newDot = document.createElement('div');
                newDot.className = `fruit-dot ${FRUIT_COLORS[nextFruit % FRUIT_COLORS.length]} pop-in`;
                newDot.id = `fruit-${nextFruit}`;
                newDot.textContent = nextFruit;
                fruitGrid.appendChild(newDot);
            }
        }

        // Update extra badge
        const extraCount = allFruits.length - fruitGrid.querySelectorAll('.fruit-dot:not(.picked)').length;
        if (extraCount > 0) {
            extraBadge.textContent = `+${extraCount}`;
            extraBadge.classList.remove('hidden');
        } else {
            extraBadge.classList.add('hidden');
        }

        // Final safety: if remaining is 0, clear everything
        if (remaining === 0) {
            fruitGrid.innerHTML = '';
            extraBadge.classList.add('hidden');
        }
    }, 600);

    updateFruitsLeft(remaining);
}

function updateFruitsLeft(n) {
    fruitsLeftLabel.textContent = `${n} Fruits Left`;
    statRemaining.textContent = n;
}

// ──────────────────────────────────────────────
// Crate Rendering
// ──────────────────────────────────────────────
function addFruitToCrate(slot, fruitId, pickerId) {
    const el = document.getElementById(`crate-slot-${slot}`);
    if (!el) return;
    const colorIdx = fruitId % FRUIT_COLORS.length;
    const bgColor = getComputedStyle(document.documentElement)
        .getPropertyValue({
            0: '--coral', 1: '--orange', 2: '--pink',
            3: '--coral', 4: '--orange', 5: '--pink'
        }[colorIdx] || '--coral').trim();
    // Use picker color for crate slot background
    const colors = ['#E07A5F', '#F2A65A', '#E0707E', '#E8A87C', '#D4776B', '#C1666B'];
    el.style.background = colors[colorIdx];
    el.textContent = fruitId;
    el.classList.add('filled', 'pop-in');
    setTimeout(() => el.classList.remove('pop-in'), 500);
    crateSlotsLabel.textContent = `${slot + 1}/${CRATE_CAPACITY} Slots`;
}

function clearCrate() {
    for (let i = 0; i < CRATE_CAPACITY; i++) {
        const el = document.getElementById(`crate-slot-${i}`);
        if (el) {
            el.style.background = '';
            el.textContent = '';
            el.classList.remove('filled', 'pop-in');
        }
    }
    crateSlotsLabel.textContent = `0/${CRATE_CAPACITY} Slots`;
}

// Explicit purge for final cleanup
function purgeActiveCrate() {
    clearCrate();
    // Additional UI reset if needed
    console.log("Crate purged.");
}

// ──────────────────────────────────────────────
// Truck Rendering
// ──────────────────────────────────────────────
function addCrateToTruck(crateId, fruits) {
    truckLabel.style.display = 'none';
    const crate = document.createElement('div');
    crate.className = 'truck-crate';
    // Add small dots to represent fruits
    const dotsContainer = document.createElement('div');
    dotsContainer.className = 'truck-crate-dots';
    const dotCount = Math.min(fruits.length, 6);
    for (let i = 0; i < dotCount; i++) {
        const d = document.createElement('div');
        d.className = 'truck-crate-dot';
        dotsContainer.appendChild(d);
    }
    crate.appendChild(dotsContainer);
    truckCratesEl.appendChild(crate);

    // Update shipped count
    const shipped = parseInt(statShipped.textContent) + fruits.length;
    statShipped.textContent = shipped;
}

// ──────────────────────────────────────────────
// Picker Status
// ──────────────────────────────────────────────
function updatePickerStatus(id, status) {
    const card = document.getElementById(`picker-card-${id}`);
    if (!card) return;
    const label = card.querySelector('.picker-label');
    card.classList.remove('active', 'done');
    if (status === 'ACTIVE') {
        card.classList.add('active');
        label.textContent = 'ACTIVE';
    } else if (status === 'DONE') {
        card.classList.add('done');
        label.textContent = 'DONE';
    } else {
        label.textContent = 'IDLE';
    }
}

// ──────────────────────────────────────────────
// Log
// ──────────────────────────────────────────────
function appendLog(time, agent, message) {
    const entry = document.createElement('div');
    entry.className = 'log-entry';
    const agentClass = `log-agent-${agent.replace(/\s/g, '')}`;
    entry.innerHTML = `<span class="log-time">[${time}]</span> <span class="log-agent ${agentClass}">${agent}:</span> ${message}`;
    // Use append because of flex-direction: column-reverse in style.css
    logList.appendChild(entry);
    // Keep max 80 entries
    while (logList.children.length > 80) {
        logList.removeChild(logList.firstChild);
    }
}

// ──────────────────────────────────────────────
// SSE Connection
// ──────────────────────────────────────────────
function connectSSE() {
    if (eventSource) eventSource.close();
    eventSource = new EventSource('/stream');

    eventSource.onmessage = function (e) {
        const msg = JSON.parse(e.data);
        handleEvent(msg);
    };

    eventSource.onerror = function () {
        // Will auto-reconnect
    };
}

function handleEvent(msg) {
    const { type, data, time } = msg;

    switch (type) {
        case 'simulation_start':
            totalFruits = data.num_fruits;
            break;

        case 'fruit_picked':
            removeFruitFromTree(data.fruit, data.remaining);
            break;

        case 'crate_update':
            addFruitToCrate(data.slot, data.fruit, data.picker);
            break;

        case 'crate_full':
            // Visual shake on crate panel
            document.getElementById('crate-panel').classList.add('shake');
            setTimeout(() => document.getElementById('crate-panel').classList.remove('shake'), 500);
            break;

        case 'truck_update':
            addCrateToTruck(data.crate_id, data.fruits);
            break;

        case 'new_crate':
            clearCrate();
            break;

        case 'lock_status':
            updateLockUI(data.resource, data.status, data.owner);
            break;

        case 'picker_status':
            updatePickerStatus(data.id, data.status);
            break;

        case 'loader_status':
            loaderStatusEl.textContent = data.status;
            break;

        case 'log':
            appendLog(data.time, data.agent, data.message);
            break;

        case 'simulation_done':
            isRunning = false;
            btnStart.classList.add('hidden');
            btnStop.classList.add('hidden');
            btnReset.classList.remove('hidden');
            purgeActiveCrate();
            break;
    }
}

// ──────────────────────────────────────────────
// Lock Status Rendering
// ──────────────────────────────────────────────
function updateLockUI(resource, status, owner) {
    const el = document.getElementById(`${resource}-lock-indicator`);
    if (!el) return;

    el.classList.remove('idle', 'locking', 'locked');

    if (status === 'LOCKING') {
        el.classList.add('locking');
        el.textContent = `${owner} LOCKING...`;
    } else if (status === 'LOCKED') {
        el.classList.add('locked');
        el.textContent = `${resource.toUpperCase()} LOCKED BY ${owner}`;
    } else {
        el.classList.add('idle');
        el.textContent = `${resource.toUpperCase()} UNLOCKED`;
    }
}

// ──────────────────────────────────────────────
// Controls
// ──────────────────────────────────────────────
async function startSimulation() {
    if (isRunning) return;

    // Get fruit count (default 52)
    const numFruits = 52;

    const resp = await fetch('/start', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ num_fruits: numFruits }),
    });
    const result = await resp.json();
    if (result.error) {
        alert(result.error);
        return;
    }

    isRunning = true;
    btnStart.classList.add('hidden');
    btnStop.classList.remove('hidden');
    btnReset.classList.add('hidden');

    // Clear UI
    logList.innerHTML = '';
    truckCratesEl.innerHTML = '';
    truckLabel.style.display = '';
    statShipped.textContent = '0';
    clearCrate();

    // Fetch initial state for tree and picker status
    const stateResp = await fetch('/state');
    const state = await stateResp.json();
    renderTree(state.tree);
    statRemaining.textContent = state.tree.length;

    // Update initial picker status
    Object.keys(state.picker_status).forEach(id => {
        updatePickerStatus(id, state.picker_status[id]);
    });

    // Connect SSE
    connectSSE();
}

async function stopSimulation() {
    await fetch('/stop', { method: 'POST' });
    isRunning = false;
    btnStart.classList.add('hidden');
    btnStop.classList.add('hidden');
    btnReset.classList.remove('hidden');
    if (eventSource) eventSource.close();
}

async function resetSimulation() {
    await fetch('/reset', { method: 'POST' });
    isRunning = false;
    if (eventSource) eventSource.close();

    // Reset UI
    btnStart.classList.remove('hidden');
    btnStop.classList.add('hidden');
    btnReset.classList.add('hidden');

    fruitGrid.innerHTML = '';
    extraBadge.classList.add('hidden');
    updateFruitsLeft(0);
    clearCrate();
    truckCratesEl.innerHTML = '';
    truckLabel.style.display = '';
    logList.innerHTML = '';
    statShipped.textContent = '0';
    statRemaining.textContent = '0';
    loaderStatusEl.textContent = 'Waiting for full slots';

    for (let i = 1; i <= 3; i++) updatePickerStatus(i, 'IDLE');
    updateLockUI('tree', 'UNLOCKED', '');
    updateLockUI('crate', 'UNLOCKED', '');
}
