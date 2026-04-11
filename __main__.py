#!/usr/bin/env python3
"""Unified service manager CLI."""

import argparse
import os
import sys
from pathlib import Path

# Add services directory to path
sys.path.insert(0, str(Path(__file__).parent))

from service_manager import ServiceManager


def get_config_path() -> str:
    """Get path to config.yaml."""
    return os.path.join(os.path.dirname(__file__), "config.yaml")


def print_status(status: dict) -> None:
    """Pretty print service status."""
    print()
    print(f"{'Service':<20} {'Status':<10} {'PID':<8} {'Port':<6}")
    print("-" * 50)
    for key, info in status.items():
        status_str = "🟢 RUNNING" if info["running"] else "⚪ STOPPED"
        pid_str = str(info["pid"]) if info["pid"] else "-"
        port_str = str(info["port"]) if info["port"] else "-"

        # Add port check indicator
        if info["running"] and info["port"] and not info["port_open"]:
            port_str += " (no listener)"

        print(f"{info['name']:<20} {status_str:<10} {pid_str:<8} {port_str:<6}")


def print_logs(logs: list[str], service_name: str = None) -> None:
    """Print log entries."""
    if not logs:
        print("No logs found." if not service_name else f"No logs for {service_name}.")
        return

    for line in logs:
        print(line.rstrip())


def main():
    parser = argparse.ArgumentParser(
        description="Unified service manager",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "action",
        choices=["status", "start", "stop", "restart", "logs", "start-all", "stop-all"],
        help="Action to perform",
    )
    parser.add_argument("service", nargs="?", help="Service name (required for start/stop/restart)")
    parser.add_argument("--reason", "-r", default="", help="Reason for the action (for logging)")
    parser.add_argument("--lines", "-n", type=int, default=50, help="Number of log lines to show")
    parser.add_argument(
        "--config",
        "-c",
        default=None,
        help="Path to config.yaml (default: ./config.yaml)",
    )

    args = parser.parse_args()

    # Get config path
    config_path = args.config or get_config_path()

    if not os.path.exists(config_path):
        print(f"Config file not found: {config_path}")
        sys.exit(1)

    # Initialize manager
    manager = ServiceManager(config_path)

    # Execute action
    if args.action == "status":
        status = manager.status()
        print_status(status)

    elif args.action in ["start", "stop", "restart"]:
        if not args.service:
            print(f"Error: {args.action} requires a service name")
            sys.exit(1)

        if args.service not in manager.services:
            print(f"Available services: {', '.join(manager.services.keys())}")
            sys.exit(1)

        if args.action == "start":
            success = manager.start(args.service, args.reason)
        elif args.action == "stop":
            success = manager.stop(args.service, args.reason)
        else:
            success = manager.restart(args.service)

        sys.exit(0 if success else 1)

    elif args.action == "logs":
        if args.service and args.service not in manager.services:
            print(f"Available services: {', '.join(manager.services.keys())}")
            sys.exit(1)

        logs = manager.logger.get_logs(args.service, args.lines)
        print_logs(logs, args.service)

    elif args.action == "start-all":
        manager.start_all()

    elif args.action == "stop-all":
        manager.stop_all()


if __name__ == "__main__":
    main()
