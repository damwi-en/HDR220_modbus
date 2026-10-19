import csv
import json
from datetime import datetime
import queue
import struct
import sys
import threading
import time

from pymodbus.client import ModbusTcpClient

# ==========================================
# [설정]
# ==========================================
ROBOT_IP = "192.168.1.149"
ROBOT_PORT = 502
MAP_FILE = "main_map.csv"
READ_INTERVAL = 0.1     # 읽기 주기 (초)
MAX_GAP = 10            # 이 값 이하로 떨어진 주소는 한 블록으로 묶어서 읽음
MAX_BLOCK = 120         # 한 번에 읽을 최대 레지스터 수 (Modbus 최대 125)
FLOAT_TOL = 1e-4        # float 비교 허용 오차

DTYPES = ("bit", "8bit", "16bit", "32bit_int", "32bit_float")

# ==========================================
# [JSON 출력 구조] 값 자리에 main_map.csv의 name을 적으면 해당 태그 값으로 치환됨
# (Meta, Errors는 build_payload()에서 자동 생성)
# ==========================================
MODEL_NAME = "Hyundai HDR220-26"
WARNING_TAG = "WarningDetail"   # LastError에 표시할 로봇 경고 코드 태그

JSON_LAYOUT = {
    "Status": {
        "Playback":      "playbackMode",
        "RemoteMode":    "remoteMode",
        "MotorState":    "motorON",
        "EmergencyStop": "Esotp",
    },
    "Joint": {
        f"J{i}": {"Position": f"pos_j{i}", "Velocity": f"v_j{i}", "Torque": f"e_j{i}"}
        for i in range(1, 7)
    },
}


def fill_layout(layout, values: dict):
    """layout의 문자열 자리를 values[태그]로 치환 (재귀). 없는 태그는 None"""
    if isinstance(layout, dict):
        return {k: fill_layout(v, values) for k, v in layout.items()}
    return values.get(layout)


# ------------------------------------------
# CSV 로드
# ------------------------------------------
def load_map(path: str) -> list[dict]:
    tags = []
    with open(path, mode="r", encoding="utf-8-sig") as f:
        for row in csv.DictReader(f):
            dtype = row["dtype"].strip().lower()
            if dtype not in DTYPES:
                raise ValueError(f"지원하지 않는 데이터형식: {dtype} ({row['name']})")
            rw = int(row["rw"])
            value = None
            if rw == 1:
                raw = row.get("value", "").strip()
                value = float(raw) if dtype == "32bit_float" else int(raw)
            tags.append({
                "name": row["name"].strip(),
                "address": int(row["address"]),
                "rw": rw,
                "start": int(row["start"] or 0),
                "dtype": dtype,
                "value": value,
                "description": row.get("description", "").strip(),
            })
    return tags


def reg_count(dtype: str) -> int:
    return 2 if dtype.startswith("32bit") else 1


def build_blocks(tags: list[dict]) -> list[tuple[int, int]]:
    """인접한 주소를 (시작주소, 개수) 블록으로 묶음"""
    spans = sorted({(t["address"], t["address"] + reg_count(t["dtype"])) for t in tags})
    blocks = []
    cur_s, cur_e = spans[0]
    for s, e in spans[1:]:
        if s - cur_e <= MAX_GAP and max(e, cur_e) - cur_s <= MAX_BLOCK:
            cur_e = max(cur_e, e)
        else:
            blocks.append((cur_s, cur_e - cur_s))
            cur_s, cur_e = s, e
    blocks.append((cur_s, cur_e - cur_s))
    return blocks


# ------------------------------------------
# 변환 (32bit는 test_joint.py와 동일하게 하위 워드 먼저: word swap)
# ------------------------------------------
def decode(regs: list[int], tag: dict):
    d, st = tag["dtype"], tag["start"]
    r = regs[0]
    if d == "bit":
        return (r >> st) & 0x1
    if d == "8bit":
        return (r >> st) & 0xFF
    if d == "16bit":
        return r
    packed = struct.pack(">HH", regs[1], regs[0])
    if d == "32bit_int":
        return struct.unpack(">i", packed)[0]
    return round(struct.unpack(">f", packed)[0], 4)


def encode(tag: dict, current: int = 0) -> list[int]:
    d, st, v = tag["dtype"], tag["start"], tag["value"]
    if d == "bit":
        return [(current & ~(1 << st)) | ((v & 1) << st)]
    if d == "8bit":
        return [(current & ~(0xFF << st)) | ((v & 0xFF) << st)]
    if d == "16bit":
        return [v & 0xFFFF]
    packed = struct.pack(">i", v) if d == "32bit_int" else struct.pack(">f", v)
    hi, lo = struct.unpack(">HH", packed)
    return [lo, hi]


def is_same(tag: dict, read_val) -> bool:
    if tag["dtype"] == "32bit_float":
        return abs(read_val - tag["value"]) < FLOAT_TOL
    return read_val == tag["value"]


# ------------------------------------------
# Modbus 작업자 (읽기 / 쓰기 스레드)
# ------------------------------------------
class ModbusWorker:
    def __init__(self, host: str, port: int, tags: list[dict], on_payload=None, echo: bool = True):
        self.client = ModbusTcpClient(host=host, port=port, timeout=2.0)
        self.on_payload = on_payload   # payload(dict)를 받을 콜백
        self.echo = echo               # stdout으로 JSON 출력 여부
        self.threads: list[threading.Thread] = []
        self.tags = tags
        self.write_tags = [t for t in tags if t["rw"] == 1]
        self.blocks = build_blocks(tags)
        self.lock = threading.Lock()
        self.write_queue: queue.Queue = queue.Queue()
        self.pending: set[str] = set()
        self.values: dict = {}
        self.stop = threading.Event()
        self.seq = 0
        self.comm_error = ""

    def build_payload(self) -> dict:
        self.seq += 1
        errors = []
        if self.comm_error:
            errors.append(self.comm_error)
        warn = self.values.get(WARNING_TAG)
        if warn:
            errors.append(f"Warning {warn}")
        return {
            "Meta": {
                "Timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "Seq": self.seq,
                "Model": MODEL_NAME,
                "Online": 1 if self.client.connected and not self.comm_error else 0,
            },
            **fill_layout(JSON_LAYOUT, self.values),
            "Errors": {"LastError": " | ".join(errors)},
        }

    def ensure_connected(self) -> bool:
        with self.lock:
            if self.client.connected:
                return True
            if self.client.connect():
                print("✅ 연결 성공", file=sys.stderr)
                return True
        print("❌ 연결 실패, 재시도...", file=sys.stderr)
        time.sleep(2.0)
        return False

    def request_write(self, tag: dict):
        if tag["name"] not in self.pending:
            self.pending.add(tag["name"])
            self.write_queue.put(tag)

    # ---- 쓰기 ----
    def write_tag(self, tag: dict) -> bool:
        with self.lock:
            current = 0
            if tag["dtype"] in ("bit", "8bit"):  # 나머지 비트 보존 (read-modify-write)
                rr = self.client.read_holding_registers(address=tag["address"], count=1)
                if rr.isError():
                    return False
                current = rr.registers[0]
            res = self.client.write_registers(address=tag["address"], values=encode(tag, current))
        return not res.isError()

    def write_loop(self):
        for t in self.write_tags:          # 시작 시 1회 전체 쓰기
            self.request_write(t)
        while not self.stop.is_set():
            try:
                tag = self.write_queue.get(timeout=0.2)
            except queue.Empty:
                continue
            if not self.ensure_connected():
                self.write_queue.put(tag)
                continue
            try:
                ok = self.write_tag(tag)
            except Exception as e:
                ok = False
                print(f"⚠️ 쓰기 예외 {tag['name']}: {e}", file=sys.stderr)
            self.pending.discard(tag["name"])
            if ok:
                print(f"✍️ 쓰기 완료 {tag['name']}={tag['value']}", file=sys.stderr)
            else:
                print(f"⚠️ 쓰기 실패 {tag['name']}", file=sys.stderr)

    # ---- 읽기 ----
    def read_once(self) -> bool:
        regs: dict[int, int] = {}
        with self.lock:
            for start, count in self.blocks:
                rr = self.client.read_holding_registers(address=start, count=count)
                if rr.isError():
                    self.comm_error = f"read fail {start}({count}): {rr}"
                    print(f"⚠️ 읽기 실패 {start}({count}): {rr}", file=sys.stderr)
                    return False
                for i, r in enumerate(rr.registers):
                    regs[start + i] = r

        for t in self.tags:
            a = t["address"]
            val = decode([regs[a + i] for i in range(reg_count(t["dtype"]))], t)
            self.values[t["name"]] = val
            if t["rw"] == 1 and not is_same(t, val):
                self.request_write(t)
        return True

    def read_loop(self):
        while not self.stop.is_set():
            t0 = time.monotonic()
            if not self.ensure_connected():
                self.comm_error = "not connected"
                if self.on_payload:
                    self.on_payload(self.build_payload())
                continue
            try:
                if self.read_once():
                    self.comm_error = ""
                payload = self.build_payload()
                if self.on_payload:
                    self.on_payload(payload)
                if self.echo:
                    print(json.dumps(payload, ensure_ascii=False), flush=True)
            except Exception as e:
                self.comm_error = f"read exception: {e}"
                print(f"⚠️ 읽기 예외: {e}", file=sys.stderr)
                with self.lock:
                    self.client.close()
            time.sleep(max(0.0, READ_INTERVAL - (time.monotonic() - t0)))

    def start(self):
        """읽기/쓰기 스레드를 백그라운드로 시작 (블로킹 없음)"""
        print(f"[설정] 태그 {len(self.tags)}개, 읽기 블록 {self.blocks}", file=sys.stderr)
        self.threads = [
            threading.Thread(target=self.write_loop, daemon=True),
            threading.Thread(target=self.read_loop, daemon=True),
        ]
        for th in self.threads:
            th.start()

    def shutdown(self):
        self.stop.set()
        for th in self.threads:
            th.join(timeout=1.0)
        self.client.close()

    def run(self):
        self.start()
        try:
            while True:
                time.sleep(0.5)
        except KeyboardInterrupt:
            print("\n[사용자 중단] 종료합니다.", file=sys.stderr)
        finally:
            self.shutdown()


def main():
    host = sys.argv[1] if len(sys.argv) > 1 else ROBOT_IP
    port = int(sys.argv[2]) if len(sys.argv) > 2 else ROBOT_PORT
    ModbusWorker(host, port, load_map(MAP_FILE)).run()


if __name__ == "__main__":
    main()
