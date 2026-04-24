"""PID file management for services."""

import os
import signal
import subprocess
from pathlib import Path
from typing import Optional


class PIDManager:
    @staticmethod
    def write_pid(pid_file: str, pid: int) -> None:
        """Write PID to file."""
        Path(pid_file).parent.mkdir(parents=True, exist_ok=True)
        with open(pid_file, "w") as f:
            f.write(str(pid))

    @staticmethod
    def read_pid(pid_file: str) -> Optional[int]:
        """Read PID from file."""
        if not os.path.exists(pid_file):
            return None
        try:
            with open(pid_file, "r") as f:
                return int(f.read().strip())
        except (ValueError, IOError):
            return None

    @staticmethod
    def is_running(pid: int) -> bool:
        """Check if a process with given PID is running and not a zombie."""
        try:
            # os.kill(pid, 0) succeeds for zombies too, so check state explicitly
            result = subprocess.run(
                ["ps", "-p", str(pid), "-o", "state="],
                capture_output=True,
                text=True,
                timeout=5,
            )
            if result.returncode != 0:
                return False  # Process not in table
            state = result.stdout.strip()
            if not state or state == "Z":
                return False  # Zombie or invalid state
            return True
        except (subprocess.TimeoutExpired, FileNotFoundError):
            return False

    @staticmethod
    def remove_pid(pid_file: str) -> None:
        """Remove PID file."""
        if os.path.exists(pid_file):
            os.remove(pid_file)

    @staticmethod
    def kill_process(pid: int, timeout: int = 10) -> bool:
        """Kill a process gracefully with SIGTERM, then SIGKILL."""
        return PIDManager.kill_process_tree(pid, timeout)

    @staticmethod
    def kill_process_tree(pid: int, timeout: int = 10) -> bool:
        """Kill a process and all its children (SIGTERM → SIGKILL)."""
        import time

        def get_children(parent_pid: int) -> list[int]:
            try:
                result = subprocess.run(
                    ["pgrep", "-P", str(parent_pid)],
                    capture_output=True, text=True, timeout=5,
                )
                return [int(p) for p in result.stdout.split() if p.strip().isdigit()]
            except Exception:
                return []

        # Collect full process tree (breadth-first)
        all_pids = [pid]
        queue = [pid]
        while queue:
            children = get_children(queue.pop(0))
            for c in children:
                if c not in all_pids:
                    all_pids.append(c)
                    queue.append(c)

        # SIGTERM all (children first, then parent)
        for p in reversed(all_pids):
            try:
                os.kill(p, signal.SIGTERM)
            except OSError:
                pass

        # Wait for graceful shutdown
        for _ in range(timeout):
            if not any(PIDManager.is_running(p) for p in all_pids):
                return True
            time.sleep(1)

        # SIGKILL stragglers
        for p in all_pids:
            try:
                if PIDManager.is_running(p):
                    os.kill(p, signal.SIGKILL)
            except OSError:
                pass

        return not any(PIDManager.is_running(p) for p in all_pids)
