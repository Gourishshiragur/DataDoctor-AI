
from __future__ import annotations

import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse, parse_qs


_HOST = "127.0.0.1"
_PORT = 8765
_started = False
_lock = threading.Lock()



def _find_active_run(mode: str = "enterprise"):
    """Return the INTERNAL DataDoctor run that currently owns a live Databricks job."""

    try:
        from storage import history
        from dbx_enterprise import jobs as dbx_jobs

        runs = history.get_runs(limit=100)

        for saved_run in runs:
            internal_run_id = str(
                saved_run.get("run_id") or ""
            ).strip()

            if not internal_run_id:
                continue

            summary = saved_run.get("summary") or {}

            if isinstance(summary, str):
                try:
                    summary = json.loads(summary)
                except Exception:
                    summary = {}

            if not isinstance(summary, dict):
                summary = {}

            run_mode = str(
                summary.get("mode") or mode
            ).strip() or mode

            remote_run_id = str(
                summary.get("dbx_run_id") or ""
            ).strip()

            # Normal authoritative state.
            saved_status = str(
                saved_run.get("status") or ""
            ).strip().lower()

            # A persisted running run is immediately active.
            if saved_status == "running":
                return {
                    "run_id": internal_run_id,
                    "mode": run_mode,
                }

            # Also recover if the local history row was prematurely marked
            # complete but the actual Databricks run is still active.
            if remote_run_id:
                try:
                    dbx = dbx_jobs.get_run_status(
                        remote_run_id,
                        mode=run_mode,
                    ) or {}

                    lifecycle = str(
                        dbx.get("life_cycle_state") or ""
                    ).upper()

                    if lifecycle in {
                        "PENDING",
                        "QUEUED",
                        "RUNNING",
                        "TERMINATING",
                    }:
                        return {
                            "run_id": internal_run_id,
                            "mode": run_mode,
                        }
                except Exception:
                    pass

    except Exception:
        pass

    return None

def _status_payload(run_id: str, mode: str):
    try:
        from ui.PipelineStudio import _get_databricks_stage_status
        from dbx_enterprise import jobs as dbx_jobs

        stages = _get_databricks_stage_status(run_id, mode)

        stage_map = {
            str(row.get("stage") or "").strip().lower(): row
            for row in stages
            if str(row.get("stage") or "").strip()
        }

        # run_id is the internal DataDoctor pipeline identity.
        # Databricks requires the persisted remote dbx_run_id.
        dbx_run_id = ""

        try:
            from storage import history
            import json

            for saved_run in history.get_runs(limit=100):
                if str(saved_run.get("run_id") or "") != str(run_id):
                    continue

                summary = saved_run.get("summary") or {}

                if isinstance(summary, str):
                    try:
                        summary = json.loads(summary)
                    except Exception:
                        summary = {}

                if isinstance(summary, dict):
                    dbx_run_id = str(
                        summary.get("dbx_run_id") or ""
                    )

                break
        except Exception:
            dbx_run_id = ""

        dbx = {}

        try:
            if dbx_run_id:
                dbx = dbx_jobs.get_run_status(
                    dbx_run_id,
                    mode=mode,
                ) or {}
        except Exception:
            dbx = {}

        def row(stage):
            return stage_map.get(stage) or {}

        bronze = row("bronze")
        profiling = row("profiling")
        quality = row("quality")
        repair = row("repair")
        silver = row("silver")
        gold = row("gold")

        def n(r, key):
            try:
                return int(r.get(key) or 0)
            except Exception:
                return 0

        # Real pipeline boundaries.
        input_rows = n(bronze, "rows_in") or n(bronze, "rows_out")

        output_rows = (
            n(gold, "rows_out")
            or n(silver, "rows_out")
            or n(repair, "rows_out")
            or n(quality, "rows_out")
            or n(bronze, "rows_out")
        )

        # These are deliberately NOT fabricated.
        quality_score = None
        repairs = None
        failed_checks = None

        payload = {
            "ok": True,
            "run_id": run_id,
            "dbx_run_id": str(dbx.get("run_id") or ""),
            "lifecycle": str(
                dbx.get("life_cycle_state") or "RUNNING"
            ),
            "result_state": str(
                dbx.get("result_state") or ""
            ),
            "stages": {
                stage: {
                    "state": str(
                        row(stage).get("state") or "waiting"
                    ).lower(),
                    "rows_in": n(row(stage), "rows_in"),
                    "rows_out": n(row(stage), "rows_out"),
                    "message": str(
                        row(stage).get("message") or ""
                    ),
                }
                for stage in (
                    "source",
                    "bronze",
                    "profiling",
                    "quality",
                    "repair",
                    "silver",
                    "gold",
                )
            },
            "metrics": {
                "rows_in": input_rows,
                "rows_out": output_rows,
                "quality_score": quality_score,
                "repairs": repairs,
                "failed_checks": failed_checks,
            },
        }

        return payload

    except Exception as exc:
        return {
            "ok": False,
            "error": str(exc),
            "stages": {},
            "metrics": {},
        }


def _active_payload():
    """Return the current persisted DataDoctor Databricks run.

    This is intentionally based on the durable history record rather
    than Streamlit session state, so navigation does not lose the run.
    """

    try:
        from storage import history

        for candidate in history.get_runs(limit=100):

            status = str(
                candidate.get("status") or ""
            ).lower()

            if status != "running":
                continue

            summary = candidate.get("summary") or {}

            if isinstance(summary, str):
                try:
                    summary = json.loads(summary)
                except Exception:
                    summary = {}

            if not isinstance(summary, dict):
                continue

            engine = str(
                summary.get("engine") or ""
            ).lower()

            backend = str(
                summary.get("backend") or ""
            ).lower()

            dbx_run_id = str(
                summary.get("dbx_run_id") or ""
            ).strip()

            if engine != "databricks" and backend != "databricks":
                continue

            return {
                "ok": True,
                "run_id": str(
                    candidate.get("run_id") or ""
                ),
                "dataset": str(
                    candidate.get("dataset")
                    or summary.get("dataset")
                    or ""
                ),
                "mode": str(
                    summary.get("mode")
                    or "enterprise"
                ),
                "dbx_run_id": dbx_run_id,
                "status": "running",
            }

        return {
            "ok": True,
            "run_id": "",
            "dataset": "",
            "mode": "enterprise",
            "dbx_run_id": "",
            "status": "idle",
        }

    except Exception as exc:

        return {
            "ok": False,
            "error": str(exc),
        }


class _Handler(BaseHTTPRequestHandler):

    def do_GET(self):
        parsed = urlparse(self.path)

        q = parse_qs(parsed.query)

        mode = str(
            (q.get("mode") or ["enterprise"])[0]
        ).strip() or "enterprise"

        # Browser-side discovery endpoint.
        # This allows Dashboard to remain mounted even when no run
        # existed at the time Streamlit rendered the page.
        if parsed.path == "/active":
            active = _find_active_run(mode)

            if active:
                self._send({
                    "ok": True,
                    "run_id": active["run_id"],
                    "mode": active["mode"],
                })
            else:
                self._send({
                    "ok": False,
                    "run_id": "",
                    "mode": mode,
                })

            return


        if parsed.path == "/active":
            self._send(_active_payload())
            return

        if parsed.path != "/status":
            self.send_response(404)
            self.end_headers()
            return

        run_id = str(
            (q.get("run_id") or [""])[0]
        ).strip()

        if not run_id:
            self._send({"ok": False, "error": "missing run_id"})
            return

        self._send(_status_payload(run_id, mode))

    def _send(self, payload):
        body = json.dumps(payload).encode("utf-8")

        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *_):
        pass


def start():
    global _started

    with _lock:
        if _started:
            return

        server = ThreadingHTTPServer(
            (_HOST, _PORT),
            _Handler,
        )

        thread = threading.Thread(
            target=server.serve_forever,
            daemon=True,
            name="datadoctor-live-status",
        )
        thread.start()

        _started = True


start()
