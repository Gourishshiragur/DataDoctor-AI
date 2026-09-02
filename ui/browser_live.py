from __future__ import annotations

import json
import streamlit.components.v1 as components


def render(run_id: str = "", mode: str = "enterprise", height: int = 430):
    """
    Browser-only live pipeline monitor.

    If run_id is empty, the browser discovers the current persisted
    Databricks run through /active.

    After discovery it polls /status every 2 seconds.

    No Streamlit rerun is used.
    """

    run_id = str(run_id or "").strip()
    mode = str(mode or "enterprise").strip()

    config = json.dumps({
        "run_id": run_id,
        "mode": mode,
    })

    html = r"""
<!doctype html>
<html>
<head>
<meta charset="utf-8">

<style>
* {
    box-sizing: border-box;
}

body {
    margin: 0;
    font-family:
        Inter, ui-sans-serif, system-ui, -apple-system,
        BlinkMacSystemFont, "Segoe UI", sans-serif;
    background: transparent;
    color: #e5e7eb;
}

.live-shell {
    width: 100%;
    border: 1px solid rgba(148,163,184,.20);
    border-radius: 22px;
    padding: 20px;
    background:
        linear-gradient(
            135deg,
            rgba(15,23,42,.97),
            rgba(30,41,59,.92)
        );
    box-shadow:
        0 18px 50px rgba(15,23,42,.18),
        inset 0 1px 0 rgba(255,255,255,.05);
}

.live-head {
    display: flex;
    justify-content: space-between;
    align-items: center;
    gap: 16px;
    margin-bottom: 18px;
}

.live-title {
    font-size: 19px;
    font-weight: 800;
}

.live-sub {
    margin-top: 5px;
    color: #94a3b8;
    font-size: 12px;
}

.live-badge {
    display: inline-flex;
    align-items: center;
    gap: 7px;
    padding: 7px 11px;
    border-radius: 999px;
    background: rgba(59,130,246,.10);
    border: 1px solid rgba(96,165,250,.22);
    color: #bfdbfe;
    font-size: 12px;
    font-weight: 800;
    white-space: nowrap;
}

.dot {
    width: 8px;
    height: 8px;
    border-radius: 50%;
    background: #60a5fa;
    box-shadow: 0 0 12px rgba(96,165,250,.8);
}

.stages {
    display: grid;
    grid-template-columns: repeat(3, 1fr);
    gap: 10px;
}

.stage {
    position: relative;
    overflow: hidden;
    min-height: 108px;
    padding: 14px;
    border-radius: 16px;
    border: 1px solid rgba(148,163,184,.16);
    background: rgba(15,23,42,.52);
    transition:
        border-color .25s ease,
        background .25s ease,
        transform .25s ease;
}

.stage.running {
    border-color: rgba(96,165,250,.62);
    background:
        linear-gradient(
            135deg,
            rgba(37,99,235,.20),
            rgba(15,23,42,.76)
        );
    transform: translateY(-1px);
}

.stage.success {
    border-color: rgba(52,211,153,.40);
}

.stage.failed {
    border-color: rgba(248,113,113,.50);
}

.stage-name {
    font-size: 13px;
    font-weight: 800;
    text-transform: capitalize;
}

.stage-state {
    margin-top: 6px;
    font-size: 11px;
    font-weight: 800;
    letter-spacing: .06em;
    text-transform: uppercase;
}

.stage-rows {
    margin-top: 12px;
    color: #94a3b8;
    font-size: 11px;
}

.message {
    margin-top: 7px;
    color: #64748b;
    font-size: 10px;
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
}

.running .stage-state {
    color: #93c5fd;
}

.success .stage-state {
    color: #6ee7b7;
}

.failed .stage-state {
    color: #fca5a5;
}

.waiting .stage-state {
    color: #64748b;
}

.progress {
    height: 3px;
    margin-top: 12px;
    border-radius: 999px;
    background: rgba(148,163,184,.10);
    overflow: hidden;
}

.progress > span {
    display: block;
    height: 100%;
    width: 0%;
    border-radius: inherit;
    background: #60a5fa;
    transition: width .35s ease;
}

.running .progress > span {
    width: 65%;
    animation: pulse 1.4s ease-in-out infinite;
}

.success .progress > span {
    width: 100%;
    background: #34d399;
}

.failed .progress > span {
    width: 100%;
    background: #f87171;
}

@keyframes pulse {
    0%,100% {
        opacity: .35;
        transform: translateX(-20%);
    }
    50% {
        opacity: 1;
        transform: translateX(45%);
    }
}

.footer {
    display: flex;
    justify-content: space-between;
    margin-top: 16px;
    color: #64748b;
    font-size: 10px;
}

.empty {
    min-height: 105px;
    display: flex;
    align-items: center;
    justify-content: center;
    border: 1px dashed rgba(148,163,184,.20);
    border-radius: 16px;
    color: #94a3b8;
    font-size: 13px;
}

@media (max-width: 850px) {
    .stages {
        grid-template-columns: repeat(2, 1fr);
    }
}
</style>
</head>

<body>

<div class="live-shell">

    <div class="live-head">
        <div>
            <div class="live-title">Live Pipeline Execution</div>
            <div class="live-sub" id="run-label">
                Discovering active DataDoctorAI pipeline...
            </div>
        </div>

        <div class="live-badge">
            <span class="dot"></span>
            <span id="lifecycle">CONNECTING</span>
        </div>
    </div>

    <div class="stages" id="stages">
        <div class="empty">
            Discovering current pipeline...
        </div>
    </div>

    <div class="footer">
        <span id="metrics">Waiting for live data</span>
        <span id="updated">Connecting...</span>
    </div>

</div>

<script>

const CONFIG = __CONFIG__;

let currentRunId = String(CONFIG.run_id || "").trim();
let currentMode = String(CONFIG.mode || "enterprise").trim();

const ORDER = [
    "bronze",
    "profiling",
    "quality",
    "repair",
    "silver",
    "gold"
];

const LABELS = {
    bronze: "Bronze",
    profiling: "Profiling",
    quality: "Quality",
    repair: "AI Repair",
    silver: "Silver",
    gold: "Gold"
};

function esc(value) {
    return String(value ?? "")
        .replaceAll("&", "&amp;")
        .replaceAll("<", "&lt;")
        .replaceAll(">", "&gt;")
        .replaceAll('"', "&quot;")
        .replaceAll("'", "&#039;");
}

function stateClass(state) {

    const s = String(state || "waiting").toLowerCase();

    if (
        ["running", "repairing", "retrying"].includes(s)
    ) {
        return "running";
    }

    if (s === "success") {
        return "success";
    }

    if (
        ["failed", "cancelled", "error"].includes(s)
    ) {
        return "failed";
    }

    return "waiting";
}

function stateLabel(state) {

    const s = String(state || "waiting").toLowerCase();

    if (s === "running") return "Running";
    if (s === "repairing") return "Repairing";
    if (s === "retrying") return "Retrying";
    if (s === "success") return "Success";
    if (s === "failed") return "Failed";
    if (s === "cancelled") return "Cancelled";

    return "Waiting";
}

function render(data) {

    const lifecycle =
        String(data.lifecycle || "RUNNING").toUpperCase();

    document.getElementById("lifecycle").textContent =
        lifecycle;

    document.getElementById("run-label").textContent =
        "DataDoctorAI run " +
        String(currentRunId).slice(0, 12) +
        (
            data.dataset
                ? " ? " + String(data.dataset)
                : ""
        );

    const stages = data.stages || {};

    document.getElementById("stages").innerHTML =
        ORDER.map(stage => {

            const item = stages[stage] || {};

            const state =
                String(item.state || "waiting").toLowerCase();

            const cls = stateClass(state);

            const rowsIn =
                Number(item.rows_in || 0);

            const rowsOut =
                Number(item.rows_out || 0);

            const message =
                item.message || "";

            return `
                <div class="stage ${cls}">

                    <div class="stage-name">
                        ${esc(LABELS[stage])}
                    </div>

                    <div class="stage-state">
                        ${esc(stateLabel(state))}
                    </div>

                    <div class="stage-rows">
                        In: ${rowsIn.toLocaleString()}
                        &nbsp;?&nbsp;
                        Out: ${rowsOut.toLocaleString()}
                    </div>

                    <div class="message">
                        ${esc(message)}
                    </div>

                    <div class="progress">
                        <span></span>
                    </div>

                </div>
            `;

        }).join("");

    const m = data.metrics || {};

    document.getElementById("metrics").textContent =
        "Rows in: " +
        Number(m.rows_in || 0).toLocaleString() +
        "  ?  Rows out: " +
        Number(m.rows_out || 0).toLocaleString();

    document.getElementById("updated").textContent =
        "Live ? " +
        new Date().toLocaleTimeString();
}

async function discoverActiveRun() {

    try {

        const response = await fetch(
            "http://127.0.0.1:8765/active?_=" +
            Date.now(),
            {
                cache: "no-store"
            }
        );

        const data = await response.json();

        if (
            data &&
            data.ok &&
            data.run_id
        ) {

            currentRunId =
                String(data.run_id);

            currentMode =
                String(data.mode || currentMode);

            return true;
        }

    } catch (err) {

        document.getElementById("lifecycle").textContent =
            "RECONNECTING";

    }

    return false;
}

async function pollStatus() {

    try {

        if (!currentRunId) {

            const found =
                await discoverActiveRun();

            if (!found) {

                document.getElementById("lifecycle").textContent =
                    "WAITING";

                document.getElementById("run-label").textContent =
                    "Waiting for an active DataDoctorAI pipeline";

                document.getElementById("stages").innerHTML =
                    `
                    <div class="empty">
                        No pipeline is currently running.
                        Monitoring for the next run...
                    </div>
                    `;

                document.getElementById("updated").textContent =
                    "Monitoring...";

                return;
            }
        }

        const url =
            "http://127.0.0.1:8765/status" +
            "?run_id=" +
            encodeURIComponent(currentRunId) +
            "&mode=" +
            encodeURIComponent(currentMode) +
            "&_=" +
            Date.now();

        const response =
            await fetch(
                url,
                {
                    cache: "no-store"
                }
            );

        const data =
            await response.json();

        if (data && data.ok) {

            render(data);

            const lifecycle =
                String(
                    data.lifecycle || ""
                ).toUpperCase();

            const result =
                String(
                    data.result_state || ""
                ).toUpperCase();

            if (
                lifecycle === "TERMINATED" ||
                lifecycle === "SKIPPED" ||
                lifecycle === "INTERNAL_ERROR"
            ) {

                /*
                 * Keep the final stage state visible briefly,
                 * then discover the next run.
                 */

                if (result === "SUCCESS") {

                    document.getElementById("lifecycle").textContent =
                        "COMPLETED";

                }

                setTimeout(() => {

                    currentRunId = "";

                }, 4000);
            }

            return;
        }

        currentRunId = "";

    } catch (err) {

        document.getElementById("lifecycle").textContent =
            "RECONNECTING";
    }
}

pollStatus();

setInterval(
    pollStatus,
    2000
);

</script>

</body>
</html>
"""

    html = html.replace(
        "__CONFIG__",
        config
    )

    components.html(
        html,
        height=height,
        scrolling=False
    )
