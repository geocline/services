"""PID file management for services."""

import os
import signal
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
        """Check if a process with given PID is running."""
        try:
            os.kill(pid, 0)
            return True
        except OSError:
            return False

    @staticmethod
    def remove_pid(pid_file: str) -> None:
        """Remove PID file."""
        if os.path.exists(pid_file):
            os.remove(pid_file)

    @staticmethod
    def kill_process(pid: int, timeout: int = 10) -> bool:
        """Kill a process gracefully with SIGTERM, then SIGKILL."""
        try:
            os.kill(pid, signal.SIGTERM)
        except OSError:
            return True  # Already dead

        # Wait for graceful shutdown
        import time
        for _ in range(timeout):
            if not PIDManager.is_running(pid):
                return True
            time.sleep(1)

        # Force kill
        try:
            os.kill(pid, signal.SIGKILL)
        except OSError:
            pass

        return not PIDManager.is_running(pid)
