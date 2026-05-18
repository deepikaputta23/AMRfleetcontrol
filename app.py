
import streamlit as st
import threading
import time
import json
import requests
import websocket
from dataclasses import dataclass
from typing import Tuple, List, Dict, Optional
from queue import Queue, Empty

HTTP_HOST = "http://192.168.128.100:8080"
WS_URL    = "ws://192.168.128.100:8081/ws"
GRID_ID = "1"
REQUEST_TIMEOUT = 8
ARRIVAL_TIMEOUT = 180
LIFTER_TIMEOUT  = 10
PRINT_CONTROLLER_REPLY = False

SK_BASE = 100000
SK_STEP = 1500

RCS_ENABLED = True
RCS_BASE = "http://192.168.128.110:8081"
HEADERS = {"Content-Type": "application/json", "Accept-Language": "en"}
HTTP_TIMEOUT_SEC = 15

TASK_MOVE_AMR = "MoveAMR"
TASK_LIFT_UP = "LiftUp"
TASK_PUT_DOWN = "PutDown"
INIT_PRIORITY = 1

POLL_INTERVAL_SEC = 0.2
POLL_TIMEOUT_SEC = 180
HORIZON = 5000
ADJACENT_STEPS = True
INCLUDE_CARRIER_EVERY_LEG = True

IDLE_SLEEP_SEC = 0.05
CYCLE_PAUSE_SEC = 1.0
AFTER_PICKUP_SEC = 0.30
AFTER_PUTDOWN_SEC = 0.30
MIN_TICK_SEC = 0.08

st.set_page_config(page_title="TAKUMI & PHOXTER LOOP", layout="wide")

class ThreadLog:
    def __init__(self, capacity: int = 2000):
        self.capacity = capacity
        self._buffer = []
        self._q = Queue()

    def write(self, msg: str):
        line = msg.rstrip("\n")
        if not line:
            return
        self._buffer.append(line)
        if len(self._buffer) > self.capacity:
            self._buffer = self._buffer[-self.capacity:]
        self._q.put(line)

    def snapshot(self) -> List[str]:
        return list(self._buffer)

    def drain(self, max_count=200) -> List[str]:
        out = []
        for _ in range(max_count):
            try:
                out.append(self._q.get_nowait())
            except Empty:
                break
        return out

if "log" not in st.session_state:
    st.session_state.log = ThreadLog(capacity=3000)

LOG = st.session_state.log
def log_info(msg: str):
    ts = time.strftime("%H:%M:%S")
    LOG.write(f"[{ts}] {msg}")


START_AT = "26"
SEQUENCE = [
    {"to": "26"},
    {"to": "11", "lifter": "up"},
    {"to": "25", "lifter": "down"},
    {"to": "12", "lifter": "up"},
    {"to": "26", "lifter": "down"},
    {"to": "25", "lifter": "up"},
    {"to": "11", "lifter": "down"},
    {"to": "26", "lifter": "up"},
    {"to": "12", "lifter": "down"},
    {"to": "26"},
]

TAKUMI_API = {
    "move": "move-grid",
    "lifter_up": "grid-lifter-up",
    "lifter_down": "grid-lifter-down",
    "stop": "grid-stop",
    "terminate": "task-terminate",
    "resume": "grid-resume", 
}

def api_get(params: dict):
    url = f"{HTTP_HOST.rstrip('/')}/api/v2"
    r = requests.get(url, params=params, timeout=REQUEST_TIMEOUT)
    r.raise_for_status()
    try:
        data = r.json()
    except Exception:
        data = {}
    err = (data or {}).get("errormessage")
    if err:
        raise RuntimeError(f"Controller error: {err}")
    if PRINT_CONTROLLER_REPLY:
        log_info(f"[Takumi CTRL] {data}")
    return data

def move_grid(grid_id: str, origin: str, dest: str):
    log_info(f"[Takumi] CMD move-grid @{origin} → @{dest}")
    return api_get({
        "type": TAKUMI_API["move"],
        "gridId": grid_id,
        "origin": f"@{origin}",
        "destinations": f"@{dest}",
    })

def grid_lifter_up(grid_id: str):
    log_info("[Takumi] CMD grid-lifter-up")
    return api_get({"type": TAKUMI_API["lifter_up"], "gridId": grid_id})

def grid_lifter_down(grid_id: str):
    log_info("[Takumi] CMD grid-lifter-down")
    return api_get({"type": TAKUMI_API["lifter_down"], "gridId": grid_id})

def grid_stop(grid_id: str):
    log_info("[Takumi] CMD grid-stop")
    try:
        return api_get({"type": TAKUMI_API["stop"], "gridId": grid_id})
    except Exception as e:
        log_info(f"[Takumi][WARN] grid-stop failed: {e}")
        return {}

def task_terminate(grid_id: str):
    log_info("[Takumi] CMD task-terminate")
    try:
        return api_get({"type": TAKUMI_API["terminate"], "gridId": grid_id})
    except Exception as e:
        log_info(f"[Takumi][WARN] task-terminate failed: {e}")
        return {}

def grid_resume(grid_id: str):
    log_info("[Takumi] CMD resume")
    try:
        return api_get({"type": TAKUMI_API["resume"], "gridId": grid_id})
    except Exception as e:
        log_info(f"[Takumi][WARN] resume failed: {e}")
        return {}


def ws_once(timeout_open=5):
    return websocket.create_connection(WS_URL, timeout=timeout_open)

def read_current_tag_via_ws(grid_id: str, max_wait=3) -> Optional[str]:
    try:
        ws = ws_once()
        ws.settimeout(1.0)
    except Exception:
        return None
    deadline = time.time() + max_wait
    try:
        while time.time() < deadline:
            try:
                data = json.loads(ws.recv())
                tag = (data.get("gridstatus", {}).get(grid_id, {}) or {}).get("tag_no_on_floor")
                if tag is not None:
                    return str(tag)
            except websocket.WebSocketTimeoutException:
                pass
            except Exception:
                break
    finally:
        try:
            ws.close()
        except Exception:
            pass
    return None

class TakumiWorker:
    """
    TakumiWorker (revised):
    - Uses an explicit step index (self._step_index) and a while loop.
    - On resume(), it will:
        1) send grid-resume (same as before),
        2) set a flag to re-issue the last move if needed (same as before),
        3) set a flag to replay the PREVIOUS step ONCE (new behavior).
    - The previous step is replayed fully (move + lifter if any), and then we continue.
    """

    def __init__(self, grid_id: str, sequence: List[Dict], start_at: str):
        self.grid_id = grid_id
        self.sequence = sequence
        self.start_at = start_at

        self.thread: Optional[threading.Thread] = None
        self._stop_after_cycle = threading.Event()
        self._force_stop = threading.Event()
        self._paused = threading.Event()
        self._running = threading.Event()
        self._continuous = False
        self._last_move_origin: Optional[str] = None
        self._last_move_dest: Optional[str] = None
        self._reissue_move_on_resume = threading.Event()
        self._replay_prev_step_once = threading.Event()

        self._resume_settle_sec = 0.2
        self._step_index: int = 0
        self._current_tag: str = self.start_at

    def is_alive(self) -> bool:
        return self.thread is not None and self.thread.is_alive()

    def start_single(self):
        self._continuous = False
        self._start()

    def start_continuous(self):
        self._continuous = True
        self._start()

    def stop_after_cycle(self):
        self._stop_after_cycle.set()
        log_info("[Takumi] Will stop after current cycle.")

    def force_stop(self):
        self._force_stop.set()
        log_info("[Takumi] FORCE STOP: task-terminate + grid-stop")
        try:
            task_terminate(self.grid_id)
            grid_stop(self.grid_id)
        finally:
            self._running.clear()

    def pause(self):
        grid_stop(self.grid_id)
        self._paused.set()
        log_info("[Takumi] TEMP STOP: paused (grid-stop).")

    def resume(self):
        grid_resume(self.grid_id)

        self._reissue_move_on_resume.set()
        self._replay_prev_step_once.set()

        self._paused.clear()
        log_info("[Takumi] RESUME: will re-send last move if needed and REPLAY previous step once.")


    def _start(self):
        if self.is_alive():
            log_info("[Takumi] Already running.")
            return

        grid_resume(self.grid_id)

        self._stop_after_cycle.clear()
        self._force_stop.clear()
        self._paused.clear()
        self._running.set()
        self._step_index = 0
        self._current_tag = read_current_tag_via_ws(self.grid_id, max_wait=3) or self.start_at

        self.thread = threading.Thread(target=self._run, daemon=True)
        self.thread.start()
        log_info("[Takumi] Worker started.")

    def _gate_pause_or_force(self) -> bool:
        """Block while paused; return True if force-stop occurred."""
        while self._paused.is_set() and not self._force_stop.is_set():
            time.sleep(0.1)
        return self._force_stop.is_set()

    def _wait_arrival(self, dest_tag: str, timeout_sec: int) -> bool:
        log_info(f"[Takumi][WAIT] Arrival @{dest_tag} (timeout {timeout_sec}s)")
        try:
            ws = ws_once()
            ws.settimeout(1.0)
        except Exception as e:
            log_info(f"[Takumi][ERROR] Cannot open WS: {e}")
            return False

        end = time.time() + timeout_sec
        try:
            while time.time() < end:
                if self._reissue_move_on_resume.is_set() and self._last_move_origin and self._last_move_dest:
                    log_info("[Takumi] Reissuing last move after resume...")
                    try:
                        if self._resume_settle_sec > 0:
                            time.sleep(self._resume_settle_sec)
                        move_grid(self.grid_id, self._last_move_origin, self._last_move_dest)
                    except Exception as e:
                        log_info(f"[Takumi][WARN] reissue move failed: {e}")
                    finally:
                        self._reissue_move_on_resume.clear()

                if self._gate_pause_or_force():
                    log_info("[Takumi] Arrival wait interrupted by force stop.")
                    return False

                try:
                    data = json.loads(ws.recv())
                    tag = (data.get("gridstatus", {}).get(self.grid_id, {}) or {}).get("tag_no_on_floor")
                    if tag is not None:
                        log_info(f"[Takumi][WS] tag=@{tag}")
                        if str(tag) == str(dest_tag):
                            log_info(f"[Takumi][OK] Arrived @{dest_tag}")
                            return True
                except websocket.WebSocketTimeoutException:
                    pass
                except Exception as e:
                    log_info(f"[Takumi][WARN] WS read: {e}")
                    time.sleep(0.2)
        finally:
            try:
                ws.close()
            except Exception:
                pass

        log_info(f"[Takumi][ABORT] Arrival timeout @{dest_tag}")
        return False

    def _wait_lifter(self, expected_up: bool, timeout_sec: int) -> bool:
        target = 1 if expected_up else 0
        log_info(f"[Takumi][WAIT] Lifter {'UP' if expected_up else 'DOWN'} ({timeout_sec}s)")
        try:
            ws = ws_once()
            ws.settimeout(1.0)
        except Exception as e:
            log_info(f"[Takumi][WARN] Cannot open WS lifter: {e}")
            return False
        end = time.time() + timeout_sec
        try:
            while time.time() < end:
                if self._gate_pause_or_force():
                    return False
                try:
                    data = json.loads(ws.recv())
                    pos = (data.get("gridstatus", {}).get(self.grid_id, {}) or {}).get("lifter_pos")
                    if pos is not None:
                        log_info(f"[Takumi][WS] lifter_pos={pos}")
                        if int(pos) == target:
                            return True
                except websocket.WebSocketTimeoutException:
                    pass
                except Exception as e:
                    log_info(f"[Takumi][WARN] WS lifter read: {e}")
                    break
        finally:
            try:
                ws.close()
            except Exception:
                pass
        log_info("[Takumi][WARN] Lifter confirm timeout")
        return False

    def _perform_lifter(self, action: str):
        if action == "up":
            grid_lifter_up(self.grid_id)
            self._wait_lifter(True, LIFTER_TIMEOUT)
        elif action == "down":
            grid_lifter_down(self.grid_id)
            self._wait_lifter(False, LIFTER_TIMEOUT)
        else:
            raise ValueError(f"Unknown lifter action: {action}")

    def _run_one_cycle(self) -> bool:
        self._current_tag = read_current_tag_via_ws(self.grid_id, max_wait=3) or self.start_at
        log_info(f"[Takumi] --- Start cycle (from @{self._current_tag}) ---")

        while self._step_index < len(self.sequence):
            if self._reissue_move_on_resume.is_set() and self._resume_settle_sec > 0:
                time.sleep(self._resume_settle_sec)
            if self._replay_prev_step_once.is_set():
                prev_idx = max(0, self._step_index - 1)
                if prev_idx != self._step_index:
                    log_info(f"[Takumi] RESUME: Replaying previous step (from index {self._step_index} -> {prev_idx}).")
                    self._step_index = prev_idx
                else:
                    log_info("[Takumi] RESUME: Already at first step; replaying step 0.")
                self._replay_prev_step_once.clear()

            step = self.sequence[self._step_index]
            dest = step["to"]
            live = read_current_tag_via_ws(self.grid_id, max_wait=1)
            origin = live or self._current_tag

            log_info(f"[Takumi] [STEP {self._step_index + 1:02d}] @{origin} → @{dest}")
            self._last_move_origin = str(origin)
            self._last_move_dest = str(dest)

            move_grid(self.grid_id, str(origin), str(dest))
            if not self._wait_arrival(str(dest), ARRIVAL_TIMEOUT):
                log_info("[Takumi][SAFETY] Move timeout — task-terminate + grid-stop")
                task_terminate(self.grid_id)
                grid_stop(self.grid_id)
                return False

            lifter = step.get("lifter")
            if lifter:
                log_info(f"[Takumi] [STEP {self._step_index + 1:02d}] lifter {lifter} @ @{dest}")
                self._perform_lifter(lifter)

            self._current_tag = str(dest)
            self._step_index += 1

            if self._force_stop.is_set():
                return False

            if self._gate_pause_or_force():
                return False

        log_info(f"[Takumi] --- Cycle complete (final @{self.sequence[-1]['to']}) ---")
        return True

    def _run(self):
        try:
            cycle_no = 0
            while self._running.is_set() and not self._force_stop.is_set():
                cycle_no += 1
                log_info(f"[Takumi] ========= CYCLE #{cycle_no} =========")
                self._step_index = 0
                ok = self._run_one_cycle()
                if not ok:
                    break
                if self._stop_after_cycle.is_set():
                    log_info("[Takumi] Stop-after-cycle requested. Sending grid-stop and exiting.")
                    grid_stop(self.grid_id)
                    break
                if not self._continuous:
                    break
        except Exception as e:
            log_info(f"[Takumi][ERROR] {e}")
            try:
                grid_stop(self.grid_id)
            except Exception:
                pass
        finally:
            self._running.clear()
            log_info("[Takumi] Worker ended.")

AMR_101 = {"name": "AMR-0100001", "robot_code": "101", "start_cell": (1, 1)}
AMR_102 = {"name": "AMR-0100002", "robot_code": "102", "start_cell": (2, 1)}
AMRS = [AMR_101, AMR_102]

CARRIER_START_POS: Dict[str, Tuple[int, int]] = {
    "0100001": (1, 3),
    "0100002": (2, 3),
}

AMR101_PROGRAM: List[Dict] = [
    {"action": "MOVE", "cell": (1, 3)},
    {"action": "LIFT", "cell": (1, 3), "carrier": "0100001"},
    {"action": "LIFT", "cell": (1, 1), "carrier": "0100001"},
    {"action": "PUT",  "cell": (0, 1), "carrier": "0100001"},
    {"action": "LIFT", "cell": (0, 1), "carrier": "0100001"},
    {"action": "LIFT", "cell": (0, 3)},
    {"action": "PUT",  "cell": (1, 3), "carrier": "0100001"},
    {"action": "MOVE", "cell": (1, 1)},
]
AMR102_PROGRAM: List[Dict] = [
    {"action": "MOVE", "cell": (2, 3)},
    {"action": "LIFT", "cell": (2, 3), "carrier": "0100002"},
    {"action": "LIFT", "cell": (2, 1)},
    {"action": "PUT",  "cell": (1, 1), "carrier": "0100002"},
    {"action": "LIFT", "cell": (1, 1), "carrier": "0100002"},
    {"action": "LIFT", "cell": (1, 3)},
    {"action": "PUT",  "cell": (2, 3), "carrier": "0100002"},
    {"action": "MOVE", "cell": (2, 1)},
]
PROGRAMS: Dict[str, List[Dict]] = {"101": AMR101_PROGRAM, "102": AMR102_PROGRAM}

def station_code_of(cell: Tuple[int, int]) -> str:
    r, c = cell
    return f"{(SK_BASE + r*SK_STEP):06d}SK{(SK_BASE + c*SK_STEP):06d}"

def next_adjacent_step(current: Tuple[int, int], target: Tuple[int, int]) -> Tuple[int, int]:
    r, c = current
    tr, tc = target
    if (r, c) == (tr, tc): return (r, c)
    if r != tr: return (r + (1 if tr > r else -1), c)
    if c != tc: return (r, c + (1 if tc > c else -1))
    return (r, c)

def choose_step(current: Tuple[int, int], target: Tuple[int, int]) -> Tuple[int, int]:
    return next_adjacent_step(current, target) if ADJACENT_STEPS else target

def rcs_post(path: str, payload: Dict) -> Dict:
    if not RCS_ENABLED:
        fake_code = f"FAKE-{int(time.time()*1000)}"
        if path.endswith("/task/submit"):
            log_info(f"[Phoxter][DRYRUN] SUBMIT {payload['taskType']} -> {fake_code}")
            return {"result": True, "code": "SUCCESS", "data": {"robotTaskCode": fake_code}}
        return {"result": True, "code": "SUCCESS", "data": {"taskStatus": "FINISHED"}}
    url = f"{RCS_BASE}{path}"
    rsp = requests.post(url, json=payload, headers=HEADERS, timeout=HTTP_TIMEOUT_SEC)
    rsp.raise_for_status()
    data = rsp.json()
    if not data.get("result", False) or data.get("code") != "SUCCESS":
        raise RuntimeError(f"[RCS] error at {path}: {data}")
    return data

def wait_all(codes: List[str]) -> Dict[str, str]:
    if not RCS_ENABLED:
        for code in codes:
            log_info(f"[Phoxter][DRYRUN] {code} -> FINISHED")
        return {c: "FINISHED" for c in codes}
    end = time.time() + POLL_TIMEOUT_SEC
    last: Dict[str, Optional[str]] = {c: None for c in codes}
    done: Dict[str, str] = {}
    while time.time() < end:
        all_finished = True
        for code in codes:
            if code in done:
                continue
            q = rcs_post("/rcs/controller/task/query", {"robotTaskCode": code})
            stt = q["data"]["taskStatus"]
            if stt != last[code]:
                log_info(f"[Phoxter] {code} -> {stt}")
                last[code] = stt
            if stt in ("FINISHED", "CANCELLED", "MANUALED"):
                done[code] = stt
            else:
                all_finished = False
        if all_finished:
            return done
        time.sleep(POLL_INTERVAL_SEC)
    raise TimeoutError(f"Timeout waiting tasks: {codes}")

def submit_moveamr(robot_code: str, to_cell: Tuple[int, int]) -> str:
    station = station_code_of(to_cell)
    payload = {
        "taskType": TASK_MOVE_AMR, "robotCode": [robot_code], "initPriority": INIT_PRIORITY,
        "targetRoute": [{"seq": 1, "type": "STATION", "code": station, "autoStart": 1}],
    }
    data = rcs_post("/rcs/controller/task/submit", payload)
    code = data["data"]["robotTaskCode"]
    log_info(f"[Phoxter] MoveAMR {robot_code} → {to_cell} | {code}")
    return code

def submit_liftup_in_place(robot_code: str, cell: Tuple[int, int], carrier: str) -> str:
    station = station_code_of(cell)
    route = [
        {"seq": 1, "type": "CARRIER", "code": carrier, "autoStart": 1},
        {"seq": 2, "type": "STATION", "code": station, "autoStart": 1},
    ]
    payload = {"taskType": TASK_LIFT_UP, "robotCode": [robot_code], "initPriority": INIT_PRIORITY, "targetRoute": route}
    data = rcs_post("/rcs/controller/task/submit", payload)
    code = data["data"]["robotTaskCode"]
    log_info(f"[Phoxter] LiftUp IN-PLACE {robot_code} @ {cell} carrier={carrier} | {code}")
    return code

def submit_liftup_move(robot_code: str, to_cell: Tuple[int, int], carrier: Optional[str]) -> str:
    station = station_code_of(to_cell)
    if INCLUDE_CARRIER_EVERY_LEG and carrier:
        route = [
            {"seq": 1, "type": "CARRIER", "code": carrier, "autoStart": 1},
            {"seq": 2, "type": "STATION", "code": station, "autoStart": 1},
        ]
    else:
        route = [{"seq": 1, "type": "STATION", "code": station, "autoStart": 1}]
    payload = {"taskType": TASK_LIFT_UP, "robotCode": [robot_code], "initPriority": INIT_PRIORITY, "targetRoute": route}
    data = rcs_post("/rcs/controller/task/submit", payload)
    code = data["data"]["robotTaskCode"]
    log_info(f"[Phoxter] LiftUp MOVE {robot_code} → {to_cell} carrier={carrier} | {code}")
    return code

def submit_putdown(robot_code: str, carrier: str, station_cell: Tuple[int, int]) -> str:
    station = station_code_of(station_cell)
    payload = {
        "taskType": TASK_PUT_DOWN, "robotCode": [robot_code], "initPriority": INIT_PRIORITY,
        "targetRoute": [
            {"seq": 1, "type": "CARRIER", "code": carrier, "autoStart": 1},
            {"seq": 2, "type": "STATION", "code": station, "autoStart": 1},
        ],
    }
    data = rcs_post("/rcs/controller/task/submit", payload)
    code = data["data"]["robotTaskCode"]
    log_info(f"[Phoxter] PutDown {robot_code} @ {station_cell} carrier={carrier} | {code}")
    return code

@dataclass
class RobotRuntime:
    name: str
    robot_code: str
    current_cell: Tuple[int, int]
    start_cell: Tuple[int, int]
    carrying: Optional[str] = None
    idx: int = 0

def build_proposal(rt: RobotRuntime, program: List[Dict]) -> Dict:
    if rt.idx >= len(program):
        return {"action": "WAIT", "from": rt.current_cell, "to": rt.current_cell,
                "carrier": None, "step_target": rt.current_cell, "step_type": "WAIT"}

    step = program[rt.idx]
    act = step["action"].upper()
    target = step["cell"]
    req_carrier = step.get("carrier")
    frm = rt.current_cell

    if act == "MOVE":
        if frm == target:
            return {"action": "WAIT", "from": frm, "to": frm, "carrier": None,
                    "step_target": target, "step_type": "MOVE"}
        to = next_adjacent_step(frm, target) if ADJACENT_STEPS else target
        return {"action": "MOVEAMR", "from": frm, "to": to, "carrier": None,
                "step_target": target, "step_type": "MOVE"}

    if act == "LIFT":
        if rt.carrying is None:
            if req_carrier is None:
                raise RuntimeError(f"{rt.name}: LIFT step missing 'carrier' for pickup.")
            if frm != target:
                to = next_adjacent_step(frm, target) if ADJACENT_STEPS else target
                return {"action": "MOVEAMR", "from": frm, "to": to,
                        "carrier": None, "step_target": target, "step_type": "LIFT"}
            return {"action": "LIFTUP_ONLY", "from": frm, "to": frm, "carrier": req_carrier,
                    "step_target": target, "step_type": "LIFT"}
        to = frm if frm == target else (next_adjacent_step(frm, target) if ADJACENT_STEPS else target)
        return {"action": "LIFTUP_MOVE", "from": frm, "to": to, "carrier": rt.carrying,
                "step_target": target, "step_type": "LIFT"}

    if act == "PUT":
        if frm != target:
            to = next_adjacent_step(frm, target) if ADJACENT_STEPS else target
            if rt.carrying:
                return {"action": "LIFTUP_MOVE", "from": frm, "to": to, "carrier": rt.carrying,
                        "step_target": target, "step_type": "PUT"}
            return {"action": "MOVEAMR", "from": frm, "to": to, "carrier": None,
                    "step_target": target, "step_type": "PUT"}
        use_carrier = rt.carrying or req_carrier
        if not use_carrier:
            raise RuntimeError(f"{rt.name}: PUT requires a carrier.")
        return {"action": "PUTDOWN", "from": frm, "to": frm, "carrier": use_carrier,
                "step_target": target, "step_type": "PUT"}

    raise ValueError(f"{rt.name}: Unknown action '{act}'")

def resolve_parallel_conflicts(proposals: Dict[str, Dict]) -> Dict[str, Tuple[int, int]]:
    order = [a["name"] for a in AMRS]
    chosen: Dict[str, Tuple[int, int]] = {n: p["to"] for n, p in proposals.items()}

    inv: Dict[Tuple[int, int], List[str]] = {}
    for n, p in proposals.items():
        inv.setdefault(p["to"], []).append(n)
    for cell, names in inv.items():
        if len(names) > 1:
            winner = min(names, key=lambda nm: order.index(nm))
            for nm in names:
                if nm != winner:
                    chosen[nm] = proposals[nm]["from"]

    names = list(proposals.keys())
    if len(names) == 2:
        a, b = names
        af, at = proposals[a]["from"], proposals[a]["to"]
        bf, bt = proposals[b]["from"], proposals[b]["to"]
        if af == bt and bf == at and af != at:
            first = min([a, b], key=lambda nm: order.index(nm))
            other = b if first == a else a
            chosen[other] = proposals[other]["from"]
    return chosen

class PhoxterWorker:
    def __init__(self):
        self.thread: Optional[threading.Thread] = None
        self._stop_after_cycle = threading.Event()
        self._force_stop = threading.Event()
        self._paused = threading.Event()
        self._running = threading.Event()
        self._continuous = False

        self.states: Dict[str, RobotRuntime] = {
            amr["name"]: RobotRuntime(
                name=amr["name"],
                robot_code=amr["robot_code"],
                current_cell=amr["start_cell"],
                start_cell=amr["start_cell"],
                carrying=None,
                idx=0
            ) for amr in AMRS
        }
        self.carrier_pos: Dict[str, Tuple[int, int]] = dict(CARRIER_START_POS)

    def is_alive(self) -> bool:
        return self.thread is not None and self.thread.is_alive()

    def start_single(self):
        self._continuous = False
        self._start()

    def start_continuous(self):
        self._continuous = True
        self._start()

    def _start(self):
        if self.is_alive():
            log_info("[Phoxter] Already running.")
            return
        self._stop_after_cycle.clear()
        self._force_stop.clear()
        self._paused.clear()
        self._running.set()
        self.thread = threading.Thread(target=self._run, daemon=True)
        self.thread.start()
        log_info("[Phoxter] Worker started.")

    def stop_after_cycle(self):
        self._stop_after_cycle.set()
        log_info("[Phoxter] Will stop after current cycle.")

    def force_stop(self):
        self._force_stop.set()
        self._running.clear()
        log_info("[Phoxter] FORCE STOP: stop issuing tasks now.")

    def pause(self):
        self._paused.set()
        log_info("[Phoxter] Pause: no new tasks will be submitted.")

    def resume(self):
        self._paused.clear()
        log_info("[Phoxter] Resume: continue submitting tasks.")

    def _run_one_cycle(self):
        log_info("[Phoxter][CYCLE] Start")
        log_info(f"[Phoxter][STATE] 0100001@{self.carrier_pos['0100001']}  0100002@{self.carrier_pos['0100002']}")
        t = 0
        while t < HORIZON and not self._force_stop.is_set():
            tick_start = time.time()

            while self._paused.is_set() and not self._force_stop.is_set():
                time.sleep(0.1)
            if self._force_stop.is_set():
                break

            proposals: Dict[str, Dict] = {}
            for amr in AMRS:
                name, code = amr["name"], amr["robot_code"]
                rt = self.states[name]
                try:
                    proposals[name] = build_proposal(rt, PROGRAMS[code])
                except Exception as e:
                    log_info(f"[Phoxter][ERROR] {e}")
                    return

            if all(self.states[a["name"]].idx >= len(PROGRAMS[a["robot_code"]]) for a in AMRS):
                break

            final_to = resolve_parallel_conflicts(proposals)

            submit_codes: List[str] = []
            for amr in AMRS:
                name, code = amr["name"], amr["robot_code"]
                prop = proposals[name]
                frm, to = prop["from"], final_to[name]
                act = prop["action"]
                car = prop.get("carrier")

                if act == "WAIT":
                    continue
                if to == frm and act not in ("LIFTUP_ONLY", "PUTDOWN"):
                    continue
                if self._paused.is_set() or self._force_stop.is_set():
                    break

                if act == "LIFTUP_ONLY":
                    submit_codes.append(submit_liftup_in_place(code, frm, car))
                elif act == "PUTDOWN":
                    submit_codes.append(submit_putdown(code, car, to))
                elif act == "MOVEAMR":
                    submit_codes.append(submit_moveamr(code, to))
                elif act == "LIFTUP_MOVE":
                    submit_codes.append(submit_liftup_move(code, to, car))

            if self._force_stop.is_set():
                break

            if submit_codes:
                try:
                    wait_all(submit_codes)
                except Exception as e:
                    log_info(f"[Phoxter][ERROR] {e}")
                    return
            else:
                time.sleep(IDLE_SLEEP_SEC)

            for amr in AMRS:
                name, code = amr["name"], amr["robot_code"]
                rt = self.states[name]
                if rt.idx >= len(PROGRAMS[code]):
                    continue

                step = PROGRAMS[code][rt.idx]
                target = step["cell"]
                act = step["action"].upper()
                frm = proposals[name]["from"]
                to = final_to[name]
                moved = (to != frm)

                if moved:
                    rt.current_cell = to

                if proposals[name]["action"] == "LIFTUP_ONLY":
                    rt.carrying = proposals[name]["carrier"]
                    if AFTER_PICKUP_SEC > 0:
                        time.sleep(AFTER_PICKUP_SEC)
                    rt.idx += 1
                    continue

                if proposals[name]["action"] == "PUTDOWN":
                    dropped_carrier = proposals[name]["carrier"]
                    self.carrier_pos[dropped_carrier] = rt.current_cell
                    rt.carrying = None
                    if AFTER_PUTDOWN_SEC > 0:
                        time.sleep(AFTER_PUTDOWN_SEC)
                    rt.idx += 1
                    continue

                if rt.current_cell == target and act in ("MOVE", "LIFT"):
                    rt.idx += 1

            if MIN_TICK_SEC and MIN_TICK_SEC > 0:
                elapsed = time.time() - tick_start
                if elapsed < MIN_TICK_SEC:
                    time.sleep(MIN_TICK_SEC - elapsed)

            t += 1

        log_info("[Phoxter][CYCLE] Complete")
        for amr in AMRS:
            name = amr["name"]
            rt = self.states[name]
            log_info(f"[Phoxter][STATE] {name} @ {rt.current_cell}")
        log_info(f"[Phoxter][STATE] 0100001@{self.carrier_pos['0100001']}  0100002@{self.carrier_pos['0100002']}")

        # Home safety
        post_codes: List[str] = []
        for amr in AMRS:
            name, code = amr["name"], amr["robot_code"]
            rt = self.states[name]
            if rt.current_cell != rt.start_cell:
                log_info(f"[Phoxter][HOME] {name} -> {rt.start_cell}")
                post_codes.append(submit_moveamr(code, rt.start_cell))
                rt.current_cell = rt.start_cell
        if post_codes:
            try:
                wait_all(post_codes)
            except Exception as e:
                log_info(f"[Phoxter][ERROR] Home wait: {e}")

    def _run(self):
        try:
            cycle = 0
            log_info("[Phoxter][CONFIG] Two-AMR scripted controller (parallel)")
            for amr in AMRS:
                log_info(f"[Phoxter][CONFIG] {amr['name']} code={amr['robot_code']} start={amr['start_cell']}")
            log_info(f"[Phoxter][CONFIG] Carriers start @ 0100001:{self.carrier_pos['0100001']}  0100002:{self.carrier_pos['0100002']}")
            log_info(f"[Phoxter][CONFIG] RCS_ENABLED={RCS_ENABLED} ADJACENT_STEPS={ADJACENT_STEPS}")

            while self._running.is_set() and not self._force_stop.is_set():
                cycle += 1
                log_info(f"[Phoxter] Cycle #{cycle}")
                for amr in AMRS:
                    s = self.states[amr["name"]]
                    s.idx = 0
                    s.carrying = None
                self._run_one_cycle()
                if self._stop_after_cycle.is_set() or not self._continuous:
                    break
                time.sleep(CYCLE_PAUSE_SEC)
        except Exception as e:
            log_info(f"[Phoxter][ERROR] {e}")
        finally:
            self._running.clear()
            log_info("[Phoxter] Worker ended.")

if "takumi_worker" not in st.session_state:
    st.session_state.takumi_worker = TakumiWorker(GRID_ID, SEQUENCE, START_AT)
if "phoxter_worker" not in st.session_state:
    st.session_state.phoxter_worker = PhoxterWorker()

tw: TakumiWorker = st.session_state.takumi_worker
pw: PhoxterWorker = st.session_state.phoxter_worker

st.title("Takumi & Phoxter – Cycle Controller")

tab1, tab2, tab3 = st.tabs(["Takumi/ 匠", "Phoxter", "Logs"])

with tab1:
    st.markdown("### 匠")

    # Row 1
    c1, c2 = st.columns([1, 1])
    with c1:
        if st.button("Loop cycle flow/連続循環動作", key="tk_loop"):
            tw.start_continuous()
    with c2:
        if st.button("Single cycle/1サイクル動作", key="tk_single"):
            tw.start_single()

    st.write("")

    # Row 2
    if st.button("Cycle stop/サイクル停止", key="tk_stop_after"):
        tw.stop_after_cycle()

    st.write("")

    # Row 3
    c3, c4 = st.columns([1, 1])
    with c3:
        if st.button("Temporary stop/一時停止", key="tk_temp_stop"):
            tw.pause()
    with c4:
        if st.button("Temporary reset/リセット", key="tk_temp_reset"):
            tw.resume()

    st.caption(f"Running: {'Yes' if tw.is_alive() else 'No'} — Takumi always sends **resume** when starting.")

with tab2:
    st.markdown("### phoxter")

    # Row 1
    c1, c2 = st.columns([1, 1])
    with c1:
        if st.button("Loop cycle flow/連続循環動作", key="px_loop"):
            pw.start_continuous()
    with c2:
        if st.button("Single cycle/1サイクル動作", key="px_single"):
            pw.start_single()

    st.write("")

    # Row 2
    if st.button("Cycle stop/サイクル停止", key="px_stop_after"):
        pw.stop_after_cycle()

    st.write("")

    # Row 3
    c3, c4 = st.columns([1, 1])
    with c3:
        if st.button("Temporary stop/一時停止", key="px_temp_stop"):
            pw.pause()
    with c4:
        if st.button("Temporary reset/リセット", key="px_temp_reset"):
            pw.resume()

    st.caption(f"Running: {'Yes' if pw.is_alive() else 'No'}")

with tab3:
    st.markdown("### Live Logs")
    colA, colB = st.columns([1, 3])
    with colA:
        if st.button("Refresh logs", key="log_refresh"):
            pass 
    with colB:
        pass

    placeholder = st.empty()
    content = "\n".join(LOG.snapshot())
    placeholder.code(content, language="text")