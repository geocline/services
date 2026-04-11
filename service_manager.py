"""Core service management logic."""

import os
import signal
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import yaml

from logger import ServiceLogger
from pid_manager import PIDManager


@dataclass
class Service:
    name: str
    display_name: str
    dir: str
    command: list[str]
    pid_file: str
    port: Optional[int] = None
    uses_venv: bool = False
    launchd_service: bool = False


class ServiceManager:
    def __init__(self, config_path: str):
        self.config_path = config_path
        self.services: dict[str, Service] = {}
        self.logger: Optional[ServiceLogger] = None
        self._load_config()

    def _load_config(self) -> None:
        with open(self.config_path, "r") as f:
            config = yaml.safe_load(f)

        log_file = config.get("log_file", "/tmp/services.log")
        self.logger = ServiceLogger(log_file)

        for key, svc in config["services"].items():
            self.services[key] = Service(
                name=key,
                display_name=svc["name"],
                dir=svc["dir"],
                command=svc["command"],
                pid_file=svc["pid_file"],
                port=svc.get("port"),
                uses_venv=svc.get("uses_venv", False),
                launchd_service=svc.get("launchd_service", False),
            )

    def status(self, service_name: Optional[str] = None) -> dict[str, dict]:
        """Get status of all services or a specific one."""
        result = {}
        services = {service_name: self.services[service_name]} if service_name else self.services

        for key, svc in services.items():
            pid = PIDManager.read_pid(svc.pid_file)
            port_open = None

            # For launchd services, check via launchctl AND port
            if svc.launchd_service:
                running = self._check_launchd_service(key)
                # Also verify via port if service has one
                if svc.port:
                    port_open = self._check_port(svc.port)
                    running = running or port_open
                if running and not pid:
                    pid = self._find_pid_by_port(svc.port) if svc.port else self._find_pid_by_dir(svc.dir)
            # For services with ports, check port first (more reliable for npm/node)
            elif svc.port:
                port_open = self._check_port(svc.port)
                running = port_open
                # If port is open but no PID, find the PID
                if running and not pid:
                    pid = self._find_pid_by_port(svc.port)
            else:
                port_open = None
                # Try to find PID by directory if the recorded PID is stale
                if pid and not PIDManager.is_running(pid):
                    pid = self._find_pid_by_dir(svc.dir)
                running = pid is not None and PIDManager.is_running(pid)

            result[key] = {
                "name": svc.display_name,
                "running": running,
                "pid": pid,
                "port": svc.port,
                "port_open": port_open,
            }

        return result

    def _find_pid_by_port(self, port: int) -> Optional[int]:
        """Find the PID of a process listening on a port."""
        try:
            result = subprocess.run(
                ["lsof", "-i", f":{port}"],
                capture_output=True,
                text=True,
                timeout=5,
            )
            if result.returncode == 0 and result.stdout.strip():
                for line in result.stdout.strip().split("\n"):
                    # Skip header line
                    if line.startswith("COMMAND"):
                        continue
                    parts = line.split()
                    if len(parts) < 2:
                        continue
                    pid = int(parts[1])
                    # Only return PIDs with LISTEN state, skip ESTABLISHED (browser connections)
                    if "(LISTEN)" in line:
                        return pid
        except (subprocess.TimeoutExpired, FileNotFoundError, ValueError):
            pass
        return None

    def _find_pid_by_dir(self, service_dir: str) -> Optional[int]:
        """Find PID of a process running from a specific directory."""
        try:
            # Search for varys (bot.py), second-brain (daemon.py), and other python daemons
            patterns = ["bot.py", "daemon.py", "daemon_control.py"]
            for pattern in patterns:
                result = subprocess.run(
                    ["pgrep", "-fl", "-f", pattern],
                    capture_output=True,
                    text=True,
                    timeout=5,
                )
                for line in result.stdout.split("\n"):
                    if pattern in line and "grep" not in line:
                        pid = int(line.split()[0])
                        # Verify by checking cwd via lsof
                        verify = subprocess.run(
                            ["lsof", "-p", str(pid)],
                            capture_output=True,
                            text=True,
                            timeout=5,
                        )
                        if service_dir in verify.stdout:
                            return pid
        except (subprocess.TimeoutExpired, FileNotFoundError, ValueError):
            pass
        return None

    def _check_port(self, port: int) -> bool:
        """Check if a port is listening."""
        try:
            result = subprocess.run(
                ["lsof", "-i", f":{port}"],
                capture_output=True,
                text=True,
                timeout=5,
            )
            return result.returncode == 0 and len(result.stdout.strip()) > 0
        except (subprocess.TimeoutExpired, FileNotFoundError):
            return False

    def _check_launchd_service(self, service_name: str) -> bool:
        """Check if a launchd service is running via launchctl."""
        # Construct launchd service label from service name
        # litellm -> com.litellm.proxy
        service_label = f"com.{service_name}.proxy"
        try:
            result = subprocess.run(
                ["launchctl", "list", service_label],
                capture_output=True,
                text=True,
                timeout=5,
            )
            # launchctl list returns 0 if found, output includes PID
            if result.returncode == 0:
                for line in result.stdout.split("\n"):
                    if line.strip() and "\t" in line:
                        parts = line.split("\t")
                        if len(parts) >= 2:
                            pid_str = parts[1].strip()
                            # PID of "-" means it's not running
                            if pid_str != "-" and pid_str != "PID":
                                return True
            return False
        except (subprocess.TimeoutExpired, FileNotFoundError):
            return False

    def _start_launchd_service(self, svc) -> bool:
        """Start a launchd service via launchctl load."""
        try:
            plist_path = f"/Users/geo/Library/LaunchAgents/com.{svc.name}.proxy.plist"
            if not os.path.exists(plist_path):
                plist_path = os.path.join(svc.dir, "com.litellm.proxy.plist")

            if os.path.exists(plist_path):
                subprocess.run(
                    ["launchctl", "load", plist_path],
                    check=True,
                    capture_output=True,
                    timeout=10,
                )
                self.logger.log("START", svc.name, f"Launchd service started via {plist_path}")
                print(f"Started {svc.display_name} via launchd")
                return True
            else:
                print(f"Launchd plist not found: {plist_path}")
                return False
        except subprocess.CalledProcessError as e:
            print(f"Failed to start {svc.display_name}: {e.stderr.decode() if e.stderr else str(e)}")
            return False
        except (subprocess.TimeoutExpired, FileNotFoundError) as e:
            print(f"Failed to start {svc.display_name}: {e}")
            return False

    def start(self, service_name: str, reason: str = "") -> bool:
        """Start a service."""
        if service_name not in self.services:
            print(f"Unknown service: {service_name}")
            return False

        svc = self.services[service_name]

        # For launchd services, start via launchctl
        if svc.launchd_service:
            return self._start_launchd_service(svc)

        # Check if already running (by PID or port)
        pid = PIDManager.read_pid(svc.pid_file)
        if svc.port and self._check_port(svc.port):
            print(f"{svc.display_name} is already running (port {svc.port} in use)")
            return False
        if pid and PIDManager.is_running(pid):
            print(f"{svc.display_name} is already running (PID: {pid})")
            return False

        try:
            # Build command with venv activation if needed
            cmd = svc.command
            if svc.uses_venv:
                venv_python = os.path.join(svc.dir, "venv", "bin", "python3")
                cmd = [venv_python] + svc.command[2:]  # Skip 'bash' and '-c'

            # Start process in new session so it survives the parent
            proc = subprocess.Popen(
                cmd,
                cwd=svc.dir,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                start_new_session=True,
            )

            # Write PID to file
            PIDManager.write_pid(svc.pid_file, proc.pid)

            reason_str = reason or f"Started {svc.display_name}"
            self.logger.log("START", service_name, reason_str)
            print(f"Started {svc.display_name} (PID: {proc.pid})")
            return True

        except Exception as e:
            print(f"Failed to start {svc.display_name}: {e}")
            PIDManager.remove_pid(svc.pid_file)
            return False

    def stop(self, service_name: str, reason: str = "") -> bool:
        """Stop a service."""
        if service_name not in self.services:
            print(f"Unknown service: {service_name}")
            return False

        svc = self.services[service_name]
        pid = PIDManager.read_pid(svc.pid_file)

        # If we have a port and no valid PID, find by port
        if svc.port:
            if not pid or not PIDManager.is_running(pid):
                pid = self._find_pid_by_port(svc.port)
            # If still no valid PID but port is in use, fail
            if not pid and self._check_port(svc.port):
                print(f"Cannot find PID for {svc.display_name} (port {svc.port} in use)")
                return False
            # If no PID and port not in use, service is not running
            if not pid:
                print(f"{svc.display_name} is not running")
                PIDManager.remove_pid(svc.pid_file)
                return False
        else:
            if not pid or not PIDManager.is_running(pid):
                print(f"{svc.display_name} is not running")
                PIDManager.remove_pid(svc.pid_file)
                return False

        reason_str = reason or "User requested stop"

        if PIDManager.kill_process(pid):
            print(f"Stopped {svc.display_name}")
            self.logger.log("STOP", service_name, reason_str)
            PIDManager.remove_pid(svc.pid_file)
            return True
        else:
            print(f"Failed to stop {svc.display_name}")
            return False

    def restart(self, service_name: str) -> bool:
        """Restart a service."""
        self.stop(service_name, "Restart requested")
        return self.start(service_name, "Restarted")

    def start_all(self) -> None:
        """Start all services."""
        for name in self.services:
            self.start(name)

    def stop_all(self) -> None:
        """Stop all services."""
        for name in self.services:
            self.stop(name)
