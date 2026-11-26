/** Written into a sandbox; kept as source so the bundled server needs no extra asset. */
export const CLOUD_THREAD_HOST_SCRIPT = String.raw`#!/usr/bin/env python3
"""T3 cloud thread host: runs the agent loop and tools inside a sandbox.

The daemon keeps a turn alive while the controlling server is offline. Every turn
event is appended to a small per-thread log, and a new controller attaches to the
daemon and replays the turn from its own cursor.
"""

import argparse
import json
import os
import queue
import re
import socket
import subprocess
import sys
import threading
import time
import traceback

PROTOCOL_VERSION = "2"
MAX_TOOL_OUTPUT = 200_000
PORT_SCAN_INTERVAL = 3
SOCKET_PREFIX = "thread-host"
DAEMON_START_TIMEOUT = 15.0


def resolve_path(raw):
    if os.path.isabs(raw):
        return raw
    return os.path.normpath(os.path.join(os.getcwd(), raw))


def truncate(text):
    if len(text) <= MAX_TOOL_OUTPUT:
        return text
    return text[:MAX_TOOL_OUTPUT] + "\n… output truncated …"


def run_bash(args, process_holder):
    command = str(args.get("command", ""))
    if not command.strip():
        return "No command provided.", 1, True
    cwd = resolve_path(str(args.get("cwd") or "."))
    process = subprocess.Popen(
        ["/bin/sh", "-lc", command],
        cwd=cwd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    process_holder["process"] = process
    try:
        stdout, stderr = process.communicate(timeout=300)
    except subprocess.TimeoutExpired:
        process.kill()
        stdout, stderr = process.communicate()
        return truncate((stdout or "") + (stderr or "") + "\nCommand timed out."), 124, True
    finally:
        process_holder["process"] = None
    output = (stdout or "") + (stderr or "")
    return truncate(output), process.returncode or 0, (process.returncode or 0) != 0


def read_file(args):
    path = resolve_path(str(args["path"]))
    with open(path, "r", encoding="utf-8", errors="replace") as handle:
        lines = handle.readlines()
    offset = max(0, int(args.get("offset") or 1) - 1)
    limit = int(args.get("limit") or 2000)
    return truncate("".join(lines[offset : offset + limit]))


def write_file(args):
    path = resolve_path(str(args["path"]))
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w", encoding="utf-8") as handle:
        handle.write(str(args.get("content", "")))
    return "Wrote " + path + "."


def edit_file(args):
    path = resolve_path(str(args["path"]))
    with open(path, "r", encoding="utf-8") as handle:
        original = handle.read()
    old = str(args.get("old_string", ""))
    if old == "":
        raise ValueError("old_string must not be empty")
    count = original.count(old)
    if count == 0:
        raise ValueError("old_string was not found in the file")
    replace_all = bool(args.get("replace_all"))
    if count > 1 and not replace_all:
        raise ValueError(
            "old_string appears " + str(count) + " times; pass replace_all or include more context"
        )
    updated = original.replace(old, str(args.get("new_string", "")), -1 if replace_all else 1)
    with open(path, "w", encoding="utf-8") as handle:
        handle.write(updated)
    return "Edited " + path + "."


def list_directory(args):
    path = resolve_path(str(args.get("path") or "."))
    entries = sorted(os.listdir(path))
    return "\n".join(entries[:2000])


def listening_ports():
    ports = set()
    for path in ("/proc/net/tcp", "/proc/net/tcp6"):
        try:
            with open(path, "r", encoding="ascii", errors="replace") as handle:
                lines = handle.readlines()[1:]
        except OSError:
            continue
        for line in lines:
            fields = line.split()
            if len(fields) < 4 or fields[3] != "0A":
                continue
            try:
                port = int(fields[1].rsplit(":", 1)[1], 16)
            except (IndexError, ValueError):
                continue
            if 0 < port < 65536:
                ports.add(port)
    return sorted(ports)


def execute_tool(name, args, process_holder):
    if name == "bash":
        return run_bash(args, process_holder)
    if name == "read_file":
        return read_file(args), 0, False
    if name == "write_file":
        return write_file(args), 0, False
    if name == "edit_file":
        return edit_file(args), 0, False
    if name == "list_directory":
        return list_directory(args), 0, False
    return "Unknown tool: " + name, 1, True


def thread_key():
    name = os.path.basename(os.getcwd()) or "thread"
    return re.sub(r"[^A-Za-z0-9_.-]", "_", name)


def state_directory():
    return os.path.dirname(os.path.abspath(__file__))


def socket_path():
    return os.path.join(state_directory(), SOCKET_PREFIX + "-" + thread_key() + ".sock")


class EventLog:
    def __init__(self, path):
        self.path = path
        self.lock = threading.Lock()
        self.entries = []
        self.seq = 0
        self.load()

    def load(self):
        try:
            with open(self.path, "r", encoding="utf-8") as handle:
                for line in handle:
                    try:
                        entry = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    if isinstance(entry, dict) and isinstance(entry.get("seq"), int):
                        self.entries.append(entry)
                        self.seq = max(self.seq, entry["seq"])
        except OSError:
            pass

    def reset(self):
        with self.lock:
            self.entries = []
            self.seq = 0
            try:
                os.makedirs(os.path.dirname(self.path), exist_ok=True)
                with open(self.path, "w", encoding="utf-8"):
                    pass
            except OSError:
                traceback.print_exc(file=sys.stderr)

    def append(self, message):
        with self.lock:
            self.seq += 1
            entry = dict(message)
            entry["seq"] = self.seq
            self.entries.append(entry)
            try:
                os.makedirs(os.path.dirname(self.path), exist_ok=True)
                with open(self.path, "a", encoding="utf-8") as handle:
                    handle.write(json.dumps(entry, separators=(",", ":")) + "\n")
            except OSError:
                traceback.print_exc(file=sys.stderr)
            return entry

    def after(self, since):
        with self.lock:
            return [entry for entry in self.entries if entry.get("seq", 0) > since]

    def latest_seq(self):
        with self.lock:
            return self.seq

    def all(self):
        with self.lock:
            return list(self.entries)


TOOLS = [
    {
        "name": "bash",
        "description": "Run a shell command in the project workspace. Use it for builds, tests, git, and inspection.",
        "input_schema": {
            "type": "object",
            "properties": {
                "command": {"type": "string", "description": "The command to run."},
                "cwd": {"type": "string", "description": "Optional directory relative to the workspace."},
            },
            "required": ["command"],
        },
    },
    {
        "name": "read_file",
        "description": "Read a text file from the workspace or an absolute path.",
        "input_schema": {
            "type": "object",
            "properties": {
                "path": {"type": "string"},
                "offset": {"type": "integer", "description": "1-based first line."},
                "limit": {"type": "integer", "description": "Maximum lines to return."},
            },
            "required": ["path"],
        },
    },
    {
        "name": "write_file",
        "description": "Write a complete text file, creating parent directories as needed.",
        "input_schema": {
            "type": "object",
            "properties": {
                "path": {"type": "string"},
                "content": {"type": "string"},
            },
            "required": ["path", "content"],
        },
    },
    {
        "name": "edit_file",
        "description": "Replace an exact string in a text file. Fails when the old text is missing or ambiguous.",
        "input_schema": {
            "type": "object",
            "properties": {
                "path": {"type": "string"},
                "old_string": {"type": "string"},
                "new_string": {"type": "string"},
                "replace_all": {"type": "boolean"},
            },
            "required": ["path", "old_string", "new_string"],
        },
    },
    {
        "name": "list_directory",
        "description": "List files and directories at a path.",
        "input_schema": {
            "type": "object",
            "properties": {"path": {"type": "string"}},
            "required": [],
        },
    },
]


class Host:
    def __init__(self, persist_events=True):
        self.inbox = queue.Queue()
        self.pending_model = {}
        self.pending_approval = {}
        self.pending_model_messages = {}
        self.pending_approval_messages = {}
        self.pending_lock = threading.Lock()
        self.active_run = None
        self.interrupt = threading.Event()
        self.process_holder = {}
        self.turn_thread = None
        self.stop = False
        self.on_stop = None
        self.lock = threading.RLock()
        self.client = None
        self.last_ports = None
        self.persist_events = persist_events
        directory = state_directory()
        self.history_path = os.path.join(directory, "history.json")
        self.event_log = EventLog(os.path.join(directory, "events-" + thread_key() + ".jsonl"))
        self.messages = self.load_messages()

    def load_messages(self):
        try:
            with open(self.history_path, "r", encoding="utf-8") as handle:
                loaded = json.load(handle)
            return loaded if isinstance(loaded, list) else []
        except Exception:  # noqa: BLE001
            return []

    def save_messages(self):
        try:
            directory = os.path.dirname(self.history_path)
            os.makedirs(directory, exist_ok=True)
            temporary = self.history_path + ".tmp"
            with open(temporary, "w", encoding="utf-8") as handle:
                json.dump(self.messages, handle, ensure_ascii=False)
            os.replace(temporary, self.history_path)
            return True
        except Exception:  # noqa: BLE001
            traceback.print_exc(file=sys.stderr)
            return False

    def send_raw(self, message):
        payload = (json.dumps(message, separators=(",", ":")) + "\n").encode("utf-8")
        with self.lock:
            client = self.client
            if client is None:
                return False
            try:
                client.sendall(payload)
                return True
            except OSError:
                self.client = None
                try:
                    client.close()
                except OSError:
                    pass
                return False

    def emit(self, message, persist=True):
        with self.lock:
            entry = self.event_log.append(message) if (persist and self.persist_events) else dict(message)
            self.send_raw(entry)

    def ready_message(self):
        entries = self.event_log.all()
        with self.pending_lock:
            pending_approval_ids = list(self.pending_approval_messages.keys())
        return {
            "type": "ready",
            "protocolVersion": PROTOCOL_VERSION,
            "pid": os.getpid(),
            "latestSeq": self.event_log.latest_seq(),
            "activeRunId": self.active_run,
            "latestEventType": entries[-1].get("type") if entries else None,
            "latestRunId": entries[-1].get("runId") if entries else None,
            "pendingApprovalIds": pending_approval_ids,
        }

    def attach(self, client, since):
        with self.lock:
            previous = self.client
            self.client = client
            if previous is not None and previous is not client:
                try:
                    previous.close()
                except OSError:
                    pass
            self.send_raw(self.ready_message())
            for entry in self.event_log.after(since):
                self.send_raw(entry)
            with self.pending_lock:
                for message in self.pending_model_messages.values():
                    self.send_raw(message)
                for message in self.pending_approval_messages.values():
                    self.send_raw(message)
            if self.last_ports is not None:
                self.send_raw({"type": "ports", "ports": [{"port": port} for port in self.last_ports]})
        try:
            stream = client.makefile("r", encoding="utf-8", errors="replace")
            for line in stream:
                line = line.strip()
                if not line:
                    continue
                try:
                    message = json.loads(line)
                except json.JSONDecodeError:
                    continue
                self.handle(message)
        except OSError:
            pass
        finally:
            with self.lock:
                if self.client is client:
                    self.client = None

    def handle(self, message):
        kind = message.get("type")
        if kind == "turn":
            self.start_turn(message)
        elif kind == "model_response" or kind == "approval_response":
            self.inbox.put(message)
        elif kind == "interrupt":
            self.cancel_turn()
        elif kind == "reset_history":
            self.reset_history()
        elif kind == "stop":
            self.cancel_turn()
            self.stop = True
            if self.on_stop is not None:
                self.on_stop()
        elif kind == "ping":
            self.send_raw({"type": "pong"})
        elif kind == "attach":
            return

    def watch_ports(self):
        while not self.stop:
            ports = listening_ports()
            if ports != self.last_ports:
                self.last_ports = ports
                self.emit({"type": "ports", "ports": [{"port": port} for port in ports]}, persist=False)
            time.sleep(PORT_SCAN_INTERVAL)

    def wait_for(self, kind, request_id, timeout=1800):
        target = self.pending_model if kind == "model" else self.pending_approval
        holder = {}
        target[request_id] = holder
        try:
            while True:
                if self.stop or self.interrupt.is_set():
                    return None
                try:
                    message = self.inbox.get(timeout=0.25)
                except queue.Empty:
                    continue
                if message is None:
                    return None
                if message.get("type") == "interrupt":
                    self.interrupt.set()
                    return None
                if kind == "model" and message.get("type") == "model_response" and message.get("id") == request_id:
                    return message
                if kind == "approval" and message.get("type") == "approval_response" and message.get("id") == request_id:
                    return message
                # Preserve unrelated messages for their own waits.
                self.inbox.put(message)
                if timeout is not None:
                    timeout -= 0.25
                    if timeout <= 0:
                        return None
        finally:
            target.pop(request_id, None)

    def request_model(self, request):
        request_id = request.pop("requestId")
        message = {
            "type": "model_request",
            "id": request_id,
            "runId": request.get("runId"),
            "request": request,
        }
        request.pop("runId", None)
        with self.pending_lock:
            self.pending_model_messages[request_id] = message
        self.emit(message, persist=False)
        try:
            response = self.wait_for("model", request_id)
        finally:
            with self.pending_lock:
                self.pending_model_messages.pop(request_id, None)
        if response is None:
            raise RuntimeError("The controlling environment cancelled the model request.")
        return response

    def request_approval(self, run_id, kind, title, prompt):
        request_id = "approval-" + str(int(time.time() * 1000)) + "-" + str(id(prompt))
        message = {
            "type": "approval_request",
            "runId": run_id,
            "id": request_id,
            "kind": kind,
            "title": title,
            "prompt": prompt,
        }
        with self.pending_lock:
            self.pending_approval_messages[request_id] = message
        self.emit(message, persist=False)
        try:
            response = self.wait_for("approval", request_id)
        finally:
            with self.pending_lock:
                self.pending_approval_messages.pop(request_id, None)
        if response is None:
            return "cancel"
        return response.get("decision", "decline")

    def needs_approval(self, mode, tool):
        if mode == "none":
            return False
        if mode == "edits":
            return tool in ("bash", "write_file", "edit_file")
        return True

    def run_turn(self, turn):
        run_id = turn["runId"]
        approval_mode = turn.get("approvalMode", "ask")
        messages = self.messages
        for entry in turn.get("history", []):
            if entry.get("text"):
                messages.append({"role": entry["role"], "content": entry["text"]})
        attachment_lines = [
            "Attached file: " + item["path"] + " (" + item.get("name", "attachment") + ")"
            for item in turn.get("attachments", [])
        ]
        user_text = turn.get("text", "")
        if attachment_lines:
            user_text = user_text + "\n\n" + "\n".join(attachment_lines)
        messages.append({"role": "user", "content": user_text})
        self.save_messages()
        self.send_event({"type": "turn_started", "runId": run_id})
        try:
            while True:
                if self.interrupt.is_set():
                    self.send_event({"type": "turn_end", "runId": run_id, "status": "cancelled"})
                    return
                response = self.request_model(
                    {
                        "requestId": "model-" + str(int(time.time() * 1000)),
                        "runId": run_id,
                        "model": turn["model"],
                        "max_tokens": turn.get("maxTokens", 8192),
                        "system": turn.get("system", ""),
                        "messages": messages,
                        "tools": TOOLS,
                    }
                )
                if not response.get("ok", False):
                    self.send_event(
                        {
                            "type": "turn_end",
                            "runId": run_id,
                            "status": "failed",
                            "error": response.get("error", "The model request failed."),
                        }
                    )
                    return
                content = response.get("content", [])
                messages.append({"role": "assistant", "content": content})
                self.save_messages()
                text = "".join(block.get("text", "") for block in content if block.get("type") == "text")
                if text:
                    self.send_event({"type": "text", "runId": run_id, "text": text})
                tool_uses = [block for block in content if block.get("type") == "tool_use"]
                if not tool_uses:
                    self.send_event({"type": "turn_end", "runId": run_id, "status": "completed"})
                    return
                results = []
                for tool in tool_uses:
                    tool_id = tool.get("id", "")
                    name = tool.get("name", "")
                    args = tool.get("input") or {}
                    self.send_event(
                        {
                            "type": "tool_start",
                            "runId": run_id,
                            "id": tool_id,
                            "name": name,
                            "input": args,
                        }
                    )
                    if self.needs_approval(approval_mode, name):
                        title = "Run command" if name == "bash" else "Change files"
                        prompt = args.get("command") or args.get("path") or name
                        kind = "command" if name == "bash" else "file-change"
                        decision = self.request_approval(run_id, kind, title, str(prompt))
                        if decision in ("decline", "cancel"):
                            results.append(
                                {
                                    "type": "tool_result",
                                    "tool_use_id": tool_id,
                                    "content": "The user declined this tool call.",
                                    "is_error": True,
                                }
                            )
                            self.send_event(
                                {
                                    "type": "tool_end",
                                    "runId": run_id,
                                    "id": tool_id,
                                    "name": name,
                                    "input": args,
                                    "output": "The user declined this tool call.",
                                    "exitCode": None,
                                    "isError": True,
                                }
                            )
                            continue
                    try:
                        output, exit_code, is_error = execute_tool(name, args, self.process_holder)
                    except Exception as error:  # noqa: BLE001
                        output = str(error)
                        exit_code = 1
                        is_error = True
                    self.send_event(
                        {
                            "type": "tool_end",
                            "runId": run_id,
                            "id": tool_id,
                            "name": name,
                            "input": args,
                            "output": output,
                            "exitCode": exit_code,
                            "isError": is_error,
                        }
                    )
                    results.append(
                        {
                            "type": "tool_result",
                            "tool_use_id": tool_id,
                            "content": output,
                            "is_error": is_error,
                        }
                    )
                messages.append({"role": "user", "content": results})
                self.save_messages()
        except Exception as error:  # noqa: BLE001
            traceback.print_exc(file=sys.stderr)
            self.send_event(
                {
                    "type": "turn_end",
                    "runId": run_id,
                    "status": "cancelled" if self.interrupt.is_set() else "failed",
                    "error": str(error),
                }
            )
        finally:
            self.active_run = None

    def send_event(self, message):
        self.emit(message, persist=True)

    def start_turn(self, turn):
        if self.turn_thread is not None and self.turn_thread.is_alive():
            self.send_event(
                {
                    "type": "turn_end",
                    "runId": turn["runId"],
                    "status": "failed",
                    "error": "The sandbox host is already running a turn.",
                }
            )
            return
        self.event_log.reset()
        self.interrupt.clear()
        self.active_run = turn["runId"]
        self.turn_thread = threading.Thread(target=self.run_turn, args=(turn,), daemon=True)
        self.turn_thread.start()

    def cancel_turn(self):
        self.interrupt.set()
        process = self.process_holder.get("process")
        if process is not None:
            try:
                process.kill()
            except Exception:  # noqa: BLE001
                pass

    def reset_history(self):
        self.messages = []
        self.event_log.reset()
        if self.save_messages():
            self.emit({"type": "history_reset", "ok": True}, persist=False)
        else:
            self.emit(
                {
                    "type": "history_reset",
                    "ok": False,
                    "error": "Could not persist the reset conversation history.",
                },
                persist=False,
            )

    def run_stdio(self):
        self.client = _StdoutClient()
        self.send_raw(self.ready_message())
        threading.Thread(target=self.watch_ports, daemon=True).start()
        for line in sys.stdin:
            line = line.strip()
            if not line:
                continue
            try:
                message = json.loads(line)
            except json.JSONDecodeError:
                continue
            self.handle(message)
            if self.stop:
                return


class _StdoutClient:
    def sendall(self, payload):
        sys.stdout.buffer.write(payload)
        sys.stdout.buffer.flush()

    def close(self):
        pass


def connect_socket(path, timeout=5.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            client = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            client.connect(path)
            return client
        except OSError:
            try:
                client.close()
            except (OSError, UnboundLocalError):
                pass
            time.sleep(0.05)
    return None


def start_daemon(script):
    path = socket_path()
    client = connect_socket(path, 0.2)
    if client is not None:
        client.close()
        return
    log_path = os.path.join(state_directory(), "daemon-" + thread_key() + ".log")
    try:
        log = open(log_path, "ab")
    except OSError:
        log = subprocess.DEVNULL
    subprocess.Popen(
        [sys.executable, "-u", script, "--daemon"],
        cwd=os.getcwd(),
        stdin=subprocess.DEVNULL,
        stdout=log,
        stderr=log,
        start_new_session=True,
        close_fds=True,
    )


def run_bridge(script, since):
    path = socket_path()
    client = connect_socket(path, 0.5)
    if client is None:
        start_daemon(script)
        client = connect_socket(path, DAEMON_START_TIMEOUT)
    if client is None:
        sys.stderr.write("The sandbox thread host did not start.\n")
        return 1
    client.sendall((json.dumps({"type": "attach", "since": since}, separators=(",", ":")) + "\n").encode("utf-8"))

    def pump_stdin():
        try:
            for line in sys.stdin:
                if not line:
                    break
                client.sendall(line.encode("utf-8"))
        except OSError:
            pass
        finally:
            try:
                client.shutdown(socket.SHUT_WR)
            except OSError:
                pass

    threading.Thread(target=pump_stdin, daemon=True).start()
    try:
        stream = client.makefile("rb")
        for line in stream:
            sys.stdout.buffer.write(line)
            sys.stdout.buffer.flush()
    except (BrokenPipeError, OSError):
        pass
    return 0


def run_daemon(script):
    path = socket_path()
    os.makedirs(os.path.dirname(path), exist_ok=True)
    try:
        os.unlink(path)
    except FileNotFoundError:
        pass
    server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    try:
        server.bind(path)
        os.chmod(path, 0o600)
    except OSError:
        server.close()
        return 0
    server.listen(4)
    server.settimeout(0.5)
    host = Host(persist_events=True)
    host.on_stop = server.close
    threading.Thread(target=host.watch_ports, daemon=True).start()

    def serve(client):
        try:
            stream = client.makefile("r", encoding="utf-8", errors="replace")
            line = stream.readline()
            if not line:
                client.close()
                return
            try:
                attach = json.loads(line)
            except json.JSONDecodeError:
                client.close()
                return
            if attach.get("type") == "stop":
                host.handle({"type": "stop"})
                client.close()
                return
            if attach.get("type") != "attach":
                client.close()
                return
            since = attach.get("since")
            if since == "latest":
                since = host.event_log.latest_seq()
            if not isinstance(since, int):
                since = 0
            host.attach(client, since)
        finally:
            try:
                client.close()
            except OSError:
                pass

    while not host.stop:
        try:
            client, _ = server.accept()
        except socket.timeout:
            continue
        except OSError:
            break
        threading.Thread(target=serve, args=(client,), daemon=True).start()
    try:
        os.unlink(path)
    except OSError:
        pass
    return 0


def stop_daemon():
    client = connect_socket(socket_path(), 2.0)
    if client is None:
        return 0
    try:
        client.sendall(b'{"type":"stop"}\n')
        client.close()
    except OSError:
        return 1
    return 0


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--daemon", action="store_true")
    parser.add_argument("--connect", nargs="?", const="latest")
    parser.add_argument("--stdio", action="store_true")
    parser.add_argument("--stop", action="store_true")
    args = parser.parse_args()
    script = os.path.abspath(__file__)
    if args.stop:
        return stop_daemon()
    if args.daemon:
        return run_daemon(script)
    if args.connect is not None:
        since = args.connect
        if isinstance(since, str) and since.isdigit():
            since = int(since)
        return run_bridge(script, since)
    Host(persist_events=True).run_stdio()
    return 0


if __name__ == "__main__":
    sys.exit(main())
`;
