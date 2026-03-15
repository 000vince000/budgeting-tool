"""Simple JSON-backed token store at ~/.budgeting_tokens.json.

Stores OAuth tokens for external API integrations (Plaid, etc.).
File permissions are set to 600 on write so only the owner can read it.
"""
import json
import os
from pathlib import Path

TOKEN_FILE = Path.home() / ".budgeting_tokens.json"


def load(service: str) -> dict | None:
    """Return stored token data for the given service, or None if not found."""
    if not TOKEN_FILE.exists():
        return None
    with open(TOKEN_FILE) as f:
        data = json.load(f)
    return data.get(service)


def save(service: str, tokens: dict) -> None:
    """Persist token data for the given service."""
    data = {}
    if TOKEN_FILE.exists() and TOKEN_FILE.stat().st_size > 0:
        with open(TOKEN_FILE) as f:
            data = json.load(f)
    data[service] = tokens
    with open(TOKEN_FILE, "w") as f:
        json.dump(data, f, indent=2)
    TOKEN_FILE.chmod(0o600)
