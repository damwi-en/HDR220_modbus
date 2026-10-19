import sys
import threading

from flask import Flask, jsonify

import data_collector as dc

# ==========================================
# [설정]
# ==========================================
HTTP_HOST = "0.0.0.0"
HTTP_PORT = 4500

app = Flask(__name__)
app.json.sort_keys = False      # JSON 키 순서 유지 (Meta, Status, Joint, Errors)

_latest: dict = {}
_lock = threading.Lock()


def upload(payload: dict):
    """data_collector가 0.1초마다 호출 → 최신 JSON을 서버 메모리에 업로드"""
    global _latest
    with _lock:
        _latest = payload


@app.get("/api/data")
def get_data():
    with _lock:
        data = _latest
    if not data:
        return jsonify({"error": "no data yet"}), 503
    return jsonify(data)


@app.get("/health")
def health():
    return jsonify({"ok": True})


def main():
    robot_ip = sys.argv[1] if len(sys.argv) > 1 else dc.ROBOT_IP
    robot_port = int(sys.argv[2]) if len(sys.argv) > 2 else dc.ROBOT_PORT

    worker = dc.ModbusWorker(robot_ip, robot_port, dc.load_map(dc.MAP_FILE),
                             on_payload=upload, echo=False)
    worker.start()
    try:
        # use_reloader=False: 리로더가 켜지면 수집기가 2번 실행됨
        app.run(host=HTTP_HOST, port=HTTP_PORT, threaded=True, use_reloader=False)
    finally:
        worker.shutdown()


if __name__ == "__main__":
    main()
