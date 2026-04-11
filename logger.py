"""Unified logging for all services."""

from datetime import datetime
from pathlib import Path
from typing import Optional


class ServiceLogger:
    def __init__(self, log_file: str):
        self.log_file = Path(log_file)
        self.log_file.parent.mkdir(parents=True, exist_ok=True)

    def log(self, action: str, service: str, reason: str = "") -> None:
        """Log an event with timestamp, action, service, and reason."""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        reason_str = f" | {reason}" if reason else ""
        line = f"{timestamp} | {action:8} | {service:15} |{reason_str}\n"

        with open(self.log_file, "a") as f:
            f.write(line)

    def get_logs(self, service: Optional[str] = None, limit: int = 50) -> list[dict]:
        """Get recent log entries as structured data, optionally filtered by service."""
        if not self.log_file.exists():
            return []

        with open(self.log_file, "r") as f:
            all_lines = f.readlines()

        # Filter by service if specified
        if service:
            all_lines = [l for l in all_lines if f"| {service}" in l]

        # Take last N lines
        lines = all_lines[-limit:]

        # Parse into structured data
        result = []
        for line in lines:
            line = line.strip()
            if not line:
                continue
            parts = line.split(" | ")
            if len(parts) >= 3:
                result.append({
                    "timestamp": parts[0],
                    "action": parts[1].strip(),
                    "service": parts[2].strip(),
                    "reason": parts[3].strip() if len(parts) > 3 else None
                })

        # Reverse to get newest first
        return list(reversed(result))
