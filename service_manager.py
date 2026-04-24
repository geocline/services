"""Core service management logic."""

import os
import re
import signal
import subprocess
import sys
import threading
import time as time_module
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
    launchd_label: Optional[str] = None
    internal_pid_file: Optional[str] = None


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
                launchd_label=svc.get("launchd_label"),
                internal_pid_file=svc.get("internal_pid_file"),
            )

    def status(self, service_name: Optional[str] = None) -> dict[str, dict]:
        """Get status of all services or a specific one."""
        result = {}
        services = {service_name: self.services[service_name]} if service_name else self.services

        for key, svc in services.items():
            pid = PIDManager.read_pid(svc.pid_file)
            if pid is None and svc.internal_pid_file:
                pid = PIDManager.read_pid(svc.internal_pid_file)
            port_open = None
            service_type = "process"
            status_state = "stopped"
            status_label = "Stopped"
            status_detail = ""
            launchd_loaded = None

            # For launchd services, check via launchctl AND port
            if svc.launchd_service:
                service_type = "launchd"
                label = self._get_launchd_label(svc)
                launchd_pid = self._get_launchd_running_pid(label)
                launchd_loaded = self._is_launchd_loaded(label)
                running = launchd_pid is not None or launchd_loaded
                if launchd_pid and not pid:
                    pid = launchd_pid
                if launchd_pid:
                    status_state = "running"
                    status_label = "Running"
                    status_detail = f"launchd job active as {label}"
                elif launchd_loaded:
                    status_state = "idle"
                    status_label = "Loaded / Idle"
                    status_detail = f"Scheduled launchd job loaded as {label}; no process is running between checks."
                else:
                    status_detail = f"launchd job {label} is not loaded"
                # Also verify via port if service has one
                if svc.port:
                    port_open = self._check_port(svc.port)
                    if port_open and not running:
                        running = True
                        status_state = "running"
                        status_label = "Running"
                        status_detail = f"Port {svc.port} is listening"
                    if running and not pid:
                        pid = self._find_pid_by_port(svc.port)
            # For services with ports, check port first (more reliable for npm/node)
            elif svc.port:
                service_type = "port"
                port_open = self._check_port(svc.port)
                running = port_open
                if running:
                    status_state = "running"
                    status_label = "Running"
                    status_detail = f"Port {svc.port} is listening"
                else:
                    status_detail = f"Port {svc.port} is not listening"
                # If port is open but no PID, find the PID
                if running and not pid:
                    pid = self._find_pid_by_port(svc.port)
            else:
                port_open = None
                # If recorded PID is dead, try internal_pid_file then directory scan
                if not pid or not PIDManager.is_running(pid):
                    if svc.internal_pid_file:
                        pid = PIDManager.read_pid(svc.internal_pid_file)
                    if not pid or not PIDManager.is_running(pid):
                        pid = self._find_pid_by_dir(svc.dir)
                running = pid is not None and PIDManager.is_running(pid)
                if running:
                    status_state = "running"
                    status_label = "Running"
                    status_detail = "Process is running"
                else:
                    status_detail = "No live process found"

            result[key] = {
                "name": svc.display_name,
                "running": running,
                "pid": pid,
                "port": svc.port,
                "port_open": port_open,
                "service_type": service_type,
                "status_state": status_state,
                "status_label": status_label,
                "status_detail": status_detail,
                "launchd_label": self._get_launchd_label(svc) if svc.launchd_service else None,
                "launchd_loaded": launchd_loaded,
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

    def _find_daemon_child(self, service_dir: str) -> Optional[int]:
        """Find the daemon child process (daemon.py), not the wrapper (daemon_control.py)."""
        try:
            result = subprocess.run(
                ["pgrep", "-fl", "-f", "daemon.py"],
                capture_output=True,
                text=True,
                timeout=5,
            )
            for line in result.stdout.split("\n"):
                if "daemon.py" in line and "grep" not in line:
                    pid = int(line.split()[0])
                    # Verify via lsof cwd
                    verify = subprocess.run(
                        ["lsof", "-p", str(pid)],
                        capture_output=True,
                        text=True,
                        timeout=5,
                    )
                    if verify.returncode == 0 and service_dir in verify.stdout:
                        if PIDManager.is_running(pid):
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

    def _get_launchd_label(self, svc) -> str:
        """Get the launchd label for a service."""
        if svc.launchd_label:
            return svc.launchd_label
        return f"com.{svc.name}.proxy"

    def _get_launchd_running_pid(self, label: str) -> Optional[int]:
        """Return the live PID for a launchd service, or None if not running.

        Strategy:
        1. `launchctl list <label>` — parses the plist-dict output for "PID" = N;
           rc != 0 means the service is not loaded at all.
        2. Fallback: scan the tab-separated `launchctl list` output (first column = PID).
        Either way we then verify the PID is actually alive via is_running().
        """
        pid: Optional[int] = None
        try:
            result = subprocess.run(
                ["launchctl", "list", label],
                capture_output=True,
                text=True,
                timeout=5,
            )
            if result.returncode != 0:
                return None  # service not loaded at all
            # Parse `"PID" = 12345;` from plist-dict output
            for line in result.stdout.splitlines():
                stripped = line.strip()
                if stripped.startswith('"PID"'):
                    # e.g.  "PID" = 45689;
                    parts = stripped.split("=")
                    if len(parts) == 2:
                        try:
                            pid = int(parts[1].strip().rstrip(";"))
                        except ValueError:
                            pass
                        break
        except (subprocess.TimeoutExpired, FileNotFoundError):
            return None

        # Fallback: tab-separated `launchctl list` scan
        if pid is None:
            try:
                result2 = subprocess.run(
                    ["launchctl", "list"],
                    capture_output=True,
                    text=True,
                    timeout=5,
                )
                for line in result2.stdout.splitlines():
                    if label in line:
                        parts = line.split()
                        if parts and parts[0] not in ("-", "0", "PID"):
                            try:
                                pid = int(parts[0])
                            except ValueError:
                                pass
                        break
            except (subprocess.TimeoutExpired, FileNotFoundError):
                pass

        if pid is None:
            return None
        return pid if PIDManager.is_running(pid) else None

    def _is_launchd_loaded(self, label: str) -> bool:
        """Return True when a launchd job is loaded, even if no process is active."""
        try:
            result = subprocess.run(
                ["launchctl", "list", label],
                capture_output=True,
                text=True,
                timeout=5,
            )
            return result.returncode == 0
        except (subprocess.TimeoutExpired, FileNotFoundError):
            return False

    def _check_launchd_service(self, svc) -> bool:
        """Check if a launchd service is loaded or has a live PID."""
        label = self._get_launchd_label(svc)
        return self._get_launchd_running_pid(label) is not None or self._is_launchd_loaded(label)

    def _stop_launchd_service(self, svc) -> bool:
        """Stop a launchd service via launchctl unload (suppresses KeepAlive restart)."""
        label = self._get_launchd_label(svc)
        plist_path = f"/Users/geo/Library/LaunchAgents/{label}.plist"
        try:
            subprocess.run(
                ["launchctl", "unload", plist_path],
                capture_output=True,
                timeout=10,
            )
            # Even if unload "fails" (not loaded), the service is stopped — treat as success
            self.logger.log("STOP", svc.name, "Launchd service unloaded")
            print(f"Stopped {svc.display_name} via launchd")
            return True
        except (subprocess.TimeoutExpired, FileNotFoundError) as e:
            print(f"Failed to stop {svc.display_name}: {e}")
            return False

    def _start_launchd_service(self, svc) -> bool:
        """Start a launchd service via launchctl load."""
        try:
            label = self._get_launchd_label(svc)
            plist_path = f"/Users/geo/Library/LaunchAgents/{label}.plist"
            if not os.path.exists(plist_path):
                plist_path = os.path.join(svc.dir, f"{label}.plist")
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
            existing_pid = self._find_pid_by_port(svc.port)
            if existing_pid and PIDManager.is_running(existing_pid):
                print(f"{svc.display_name} is already running (port {svc.port}, PID: {existing_pid})")
                return False
            # Port occupied by dead/stuck process — force clear it
            if existing_pid:
                PIDManager.kill_process_tree(existing_pid)
            time_module.sleep(0.5)
            if self._check_port(svc.port):
                print(f"{svc.display_name}: port {svc.port} still occupied after cleanup")
                return False
        if pid and PIDManager.is_running(pid):
            print(f"{svc.display_name} is already running (PID: {pid})")
            return False

        def _try_start():
            # Ensure homebrew/local bin paths are available — launchd strips PATH
            _env = os.environ.copy()
            _extra = ["/opt/homebrew/bin", "/opt/homebrew/sbin", "/usr/local/bin"]
            _cur = _env.get("PATH", "")
            _env["PATH"] = ":".join(p for p in _extra if p not in _cur) + (":" if _cur else "") + _cur

            proc = subprocess.Popen(
                cmd,
                cwd=svc.dir,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                env=_env,
            )
            result = {}
            done = threading.Event()

            def run():
                try:
                    stdout_bytes, stderr_bytes = proc.communicate(timeout=15)
                except subprocess.TimeoutExpired:
                    proc.kill()
                    stdout_bytes, _ = proc.communicate()
                    stderr_bytes = b""
                result["stdout"] = stdout_bytes
                result["stderr"] = stderr_bytes
                done.set()

            t = threading.Thread(target=run, daemon=True)
            t.start()

            # Wait for subprocess to finish (it exits quickly once daemon forks off)
            exited = done.wait(timeout=5)
            if not exited:
                # daemon_control.py is still running (waiting for daemon child to exit)
                # Don't kill the wrapper — that kills the whole process group
                # Instead, scan for the daemon child directly
                # The daemon child has daemon.py in its command line; the wrapper doesn't
                daemon_pid = self._find_daemon_child(svc.dir)
                if daemon_pid:
                    return daemon_pid, None

            stdout_str = result.get("stdout", b"").decode(errors="replace")
            stderr_str = result.get("stderr", b"").decode(errors="replace")
            combined = stdout_str + stderr_str

            # Parse "Daemon started with PID X" → success
            for line in combined.split("\n"):
                m = re.search(r"D(?:aemon)? started with PID (\d+)", line, re.IGNORECASE)
                if m:
                    pid = int(m.group(1))
                    if PIDManager.is_running(pid):
                        return pid, None

            # Parse "Daemon already running with PID X" → check if actually alive
            for line in combined.split("\n"):
                m = re.search(r"D(?:aemon)? already running with PID (\d+)", line, re.IGNORECASE)
                if m:
                    pid = int(m.group(1))
                    if PIDManager.is_running(pid):
                        return pid, None
                    # Stale internal PID — clear it and retry
                    internal_pid_file = os.path.join(svc.dir, ".agents", "skills", "vault-daemon", "daemon.pid")
                    if os.path.exists(internal_pid_file):
                        os.remove(internal_pid_file)
                    return None, "stale_pid"

            # No recognized PID → try directory scan
            time_module.sleep(0.5)
            pid = self._find_pid_by_dir(svc.dir)
            if pid and PIDManager.is_running(pid):
                return pid, None

            return None, None

        try:
            # Build command with venv activation if needed
            cmd = svc.command
            if svc.uses_venv:
                venv_python = os.path.join(svc.dir, "venv", "bin", "python3")
                cmd = [venv_python] + svc.command[2:]  # Skip 'bash' and '-c'

            # First attempt
            pid_attempt1, err_attempt1 = _try_start()

            # If stale internal PID, retry after clearing it
            pid_attempt2, err_attempt2 = None, None
            if err_attempt1 == "stale_pid":
                pid_attempt2, err_attempt2 = _try_start()

            # Use the best available result
            real_pid = pid_attempt2 if pid_attempt2 else pid_attempt1
            error = err_attempt2 if pid_attempt2 else err_attempt1

            # Fallback: find PID by port (for long-running processes like npm/concurrently)
            if not real_pid and not error and svc.port:
                time_module.sleep(1)
                if self._check_port(svc.port):
                    real_pid = self._find_pid_by_port(svc.port)

            # Fallback: find PID by directory scan
            if not real_pid and not error:
                time_module.sleep(0.5)
                pid = self._find_pid_by_dir(svc.dir)
                if pid and PIDManager.is_running(pid):
                    real_pid = pid

            if not real_pid and error:
                print(error)
                return False

            # Prefer the PID the daemon wrote itself (avoids fork wrapper mismatch)
            if svc.internal_pid_file:
                internal_pid = PIDManager.read_pid(svc.internal_pid_file)
                if internal_pid and PIDManager.is_running(internal_pid):
                    real_pid = internal_pid

            PIDManager.write_pid(svc.pid_file, real_pid)
            reason_str = reason or f"Started {svc.display_name}"
            self.logger.log("START", service_name, reason_str)
            print(f"Started {svc.display_name} (PID: {real_pid})")
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

        # Launchd services must be unloaded — killing the PID alone triggers KeepAlive restart
        if svc.launchd_service:
            return self._stop_launchd_service(svc)

        pid = None
        if svc.internal_pid_file:
            pid = PIDManager.read_pid(svc.internal_pid_file)
        if pid is None:
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
            # No port: PID file may be stale/zombie, try to find by directory scan
            if not pid or not PIDManager.is_running(pid):
                pid = self._find_pid_by_dir(svc.dir)
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
        """Restart a service, force-clearing any stuck processes."""
        svc = self.services.get(service_name)
        if not svc:
            return False

        self.stop(service_name, "Restart requested")

        # Force-clear anything still holding the port
        if svc.port and self._check_port(svc.port):
            pid = self._find_pid_by_port(svc.port)
            if pid:
                PIDManager.kill_process_tree(pid)
            time_module.sleep(0.5)

        # Force-clear anything still running from this directory
        if not svc.launchd_service:
            pid = self._find_pid_by_dir(svc.dir)
            if pid and PIDManager.is_running(pid):
                PIDManager.kill_process_tree(pid)
                time_module.sleep(0.5)

        PIDManager.remove_pid(svc.pid_file)
        return self.start(service_name, "Restarted")

    def _force_kill_service(self, svc) -> None:
        """Kill a service by every available method — used during reboot.

        Steps:
        1. launchctl unload (launchd services only) — must come first to suppress KeepAlive
        2. Kill via internal_pid_file
        3. Kill via pid_file
        4. Kill via port lsof
        5. Kill via directory scan
        6. Broad pgrep sweep against service directory (catches orphaned children)
        7. Clean up PID files
        """
        killed_pids: set[int] = set()

        def _kill(pid: int) -> None:
            if pid and pid not in killed_pids:
                PIDManager.kill_process_tree(pid, timeout=5)
                killed_pids.add(pid)

        # 1. Launchd unload (suppresses KeepAlive restart)
        if svc.launchd_service:
            label = self._get_launchd_label(svc)
            plist_path = f"/Users/geo/Library/LaunchAgents/{label}.plist"
            try:
                subprocess.run(
                    ["launchctl", "unload", plist_path],
                    capture_output=True,
                    timeout=10,
                )
            except Exception:
                pass
            launchd_pid = self._get_launchd_running_pid(label)
            if launchd_pid:
                _kill(launchd_pid)

        # 2. Internal PID file
        if svc.internal_pid_file:
            pid = PIDManager.read_pid(svc.internal_pid_file)
            if pid:
                _kill(pid)

        # 3. PID file
        pid = PIDManager.read_pid(svc.pid_file)
        if pid:
            _kill(pid)

        # 4. Port-based
        if svc.port:
            pid = self._find_pid_by_port(svc.port)
            if pid:
                _kill(pid)

        # 5. Directory scan (pattern-based)
        pid = self._find_pid_by_dir(svc.dir)
        if pid:
            _kill(pid)

        # 6. Broad pgrep sweep — catches orphaned children and strays
        try:
            result = subprocess.run(
                ["pgrep", "-f", svc.dir],
                capture_output=True,
                text=True,
                timeout=5,
            )
            for line in result.stdout.splitlines():
                line = line.strip()
                if line.isdigit():
                    _kill(int(line))
        except Exception:
            pass

        # 7. Cleanup PID files
        PIDManager.remove_pid(svc.pid_file)
        if svc.internal_pid_file:
            PIDManager.remove_pid(svc.internal_pid_file)

    def _wait_until_running(self, name: str, timeout: int = 30) -> bool:
        """Poll status() until the service is confirmed running, or timeout expires."""
        for _ in range(timeout):
            try:
                if self.status(name).get(name, {}).get("running"):
                    return True
            except Exception:
                pass
            time_module.sleep(1)
        return False

    def reboot_all(self) -> dict[str, bool]:
        """Kill all services then start them in series, verifying each launch.

        Phase 1: Force-kill every service via all available methods.
        Phase 2: Start each service in config order; verify it's actually running
                 before proceeding to the next one.

        Returns: dict[service_name -> confirmed_running]
        """
        results: dict[str, bool] = {}

        # --- Phase 1: Kill everything ---
        self.logger.log("REBOOT", "all", "Phase 1: killing all services")
        print("=== REBOOT Phase 1: Killing all services ===")
        for name, svc in self.services.items():
            print(f"  Killing {svc.display_name}...")
            self._force_kill_service(svc)

        print("  Settling (3s)...")
        time_module.sleep(3)

        # --- Phase 2: Start in order, verify each ---
        self.logger.log("REBOOT", "all", "Phase 2: starting services in series")
        print("=== REBOOT Phase 2: Starting services ===")
        for name, svc in self.services.items():
            print(f"  Starting {svc.display_name}...")
            started = self.start(name, "Reboot")

            if started:
                # Port/launchd services need more time to come up
                wait = 25 if (svc.launchd_service or svc.port) else 12
                confirmed = self._wait_until_running(name, timeout=wait)
            else:
                confirmed = False

            results[name] = confirmed
            icon = "✓" if confirmed else "✗"
            print(f"  {icon} {svc.display_name}: {'running' if confirmed else 'failed'}")

        running_count = sum(1 for ok in results.values() if ok)
        self.logger.log("REBOOT", "all", f"Complete: {running_count}/{len(results)} running")
        return results

    def start_all(self) -> None:
        """Start all services."""
        for name in self.services:
            self.start(name)

    def stop_all(self) -> None:
        """Stop all services."""
        for name in self.services:
            self.stop(name)
