/** Written into a sandbox; kept as source so the bundled server needs no extra asset. */
export const CLOUD_THREAD_HOST_SCRIPT = String.raw`#!/usr/bin/env python3
"""T3 cloud thread host: runs the agent loop and tools inside a sandbox."""

import json
import os
import queue
import re
import subprocess
import sys
import threading
import time
import traceback

PROTOCOL_VERSION = "1"
MAX_TOOL_OUTPUT = 200_000
PORT_SCAN_INTERVAL = 3
_write_lock = threading.Lock()


def emit(message):
    with _write_lock:
        sys.stdout.write(json.dumps(message, separators=(",", ":")) + "\n")
        sys.stdout.flush()


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
    return f"Wrote {path}."


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
            f"old_string appears {count} times; pass replace_all or include more context"
        )
    updated = original.replace(old, str(args.get("new_string", "")), -1 if replace_all else 1)
    with open(path, "w", encoding="utf-8") as handle:
        handle.write(updated)
    return f"Edited {path}."


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
    return f"Unknown tool: {name}", 1, True


class Host:
    def __init__(self):
        self.inbox = queue.Queue()
        self.pending_model = {}
        self.pending_approval = {}
        self.active_run = None
        self.interrupt = threading.Event()
        self.process_holder = {}
        self.turn_thread = None
        self.stop = False
        self.history_path = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), "history.json"
        )
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
        except Exception:  # noqa: BLE001
            traceback.print_exc(file=sys.stderr)

    def send(self, message):
        emit(message)

    def watch_ports(self):
        last = None
        while not self.stop:
            ports = listening_ports()
            if ports != last:
                last = ports
                self.send(
                    {"type": "ports", "ports": [{"port": port} for port in ports]}
                )
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
        self.send({"type": "model_request", "id": request_id, "request": request})
        response = self.wait_for("model", request_id)
        if response is None:
            raise RuntimeError("The controlling environment cancelled the model request.")
        return response

    def request_approval(self, run_id, kind, title, prompt):
        request_id = "approval-" + str(int(time.time() * 1000)) + "-" + str(id(prompt))
        self.send(
            {
                "type": "approval_request",
                "runId": run_id,
                "id": request_id,
                "kind": kind,
                "title": title,
                "prompt": prompt,
            }
        )
        response = self.wait_for("approval", request_id)
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
            f"Attached file: {item['path']} ({item.get('name', 'attachment')})"
            for item in turn.get("attachments", [])
        ]
        user_text = turn.get("text", "")
        if attachment_lines:
            user_text = user_text + "\n\n" + "\n".join(attachment_lines)
        messages.append({"role": "user", "content": user_text})
        self.save_messages()
        self.send({"type": "turn_started", "runId": run_id})
        try:
            while True:
                if self.interrupt.is_set():
                    self.send({"type": "turn_end", "runId": run_id, "status": "cancelled"})
                    return
                response = self.request_model(
                    {
                        "requestId": "model-" + str(int(time.time() * 1000)),
                        "model": turn["model"],
                        "max_tokens": turn.get("maxTokens", 8192),
                        "system": turn.get("system", ""),
                        "messages": messages,
                        "tools": TOOLS,
                    }
                )
                if not response.get("ok", False):
                    self.send(
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
                    self.send({"type": "text", "runId": run_id, "text": text})
                tool_uses = [block for block in content if block.get("type") == "tool_use"]
                if not tool_uses:
                    self.send({"type": "turn_end", "runId": run_id, "status": "completed"})
                    return
                results = []
                for tool in tool_uses:
                    tool_id = tool.get("id", "")
                    name = tool.get("name", "")
                    args = tool.get("input") or {}
                    self.send(
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
                            self.send(
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
                    self.send(
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
            self.send(
                {
                    "type": "turn_end",
                    "runId": run_id,
                    "status": "cancelled" if self.interrupt.is_set() else "failed",
                    "error": str(error),
                }
            )

    def start_turn(self, turn):
        if self.turn_thread is not None and self.turn_thread.is_alive():
            self.send(
                {
                    "type": "turn_end",
                    "runId": turn["runId"],
                    "status": "failed",
                    "error": "The sandbox host is already running a turn.",
                }
            )
            return
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

    def loop(self):
        self.send({"type": "ready", "protocolVersion": PROTOCOL_VERSION, "pid": os.getpid()})
        threading.Thread(target=self.watch_ports, daemon=True).start()
        for line in sys.stdin:
            line = line.strip()
            if not line:
                continue
            try:
                message = json.loads(line)
            except json.JSONDecodeError:
                continue
            kind = message.get("type")
            if kind == "turn":
                self.start_turn(message)
            elif kind == "model_response" or kind == "approval_response":
                self.inbox.put(message)
            elif kind == "interrupt":
                self.cancel_turn()
            elif kind == "stop":
                self.cancel_turn()
                self.stop = True
                return
            elif kind == "ping":
                self.send({"type": "pong"})


if __name__ == "__main__":
    Host().loop()
`;
