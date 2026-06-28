"""
Consistent, versioned backups of the canonical DuckDB file with best-effort
offsite sync to Google Drive via rclone.

Design principles:
  - The local snapshot is the source of truth; offsite sync is best-effort and
    must never block or break the caller (e.g. app exit) if rclone/network fail.
  - The canonical DB is opened READ_ONLY for the snapshot, so this path can
    never mutate it.
  - Snapshots are versioned (timestamped) and pruned by retention, so a bad
    bulk edit or corruption does not propagate to the only copy.
  - A snapshot is skipped when the DB is unchanged since the last backup, but
    the offsite sync still runs so a previously-failed upload self-heals.

Run standalone for a manual/cron backup:
    python backup.py
"""

import os
import glob
import shutil
import hashlib
import datetime
import subprocess

import duckdb

DB_PATH = "budgeting-tool.db"
BACKUPS_DIR = "backups"
RETENTION = 30  # number of local snapshots to keep
# Override the Drive destination with the BUDGET_BACKUP_REMOTE env var.
# Format is an rclone remote: "<remote-name>:<path>".
DEFAULT_REMOTE = os.environ.get("BUDGET_BACKUP_REMOTE", "gdrive:budgeting-backups")
HASH_FILE = ".last_hash"
SYNC_TIMEOUT_SECONDS = 120


def file_hash(path):
    """SHA-256 of a file, read in chunks to avoid loading it all into memory."""
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for block in iter(lambda: f.read(65536), b""):
            h.update(block)
    return h.hexdigest()


def _read_last_hash(backups_dir):
    path = os.path.join(backups_dir, HASH_FILE)
    if os.path.exists(path):
        with open(path, "r") as f:
            return f.read().strip()
    return None


def _write_last_hash(backups_dir, value):
    with open(os.path.join(backups_dir, HASH_FILE), "w") as f:
        f.write(value)


def snapshot(db_path, backups_dir):
    """
    Write a standalone copy of db_path into backups_dir.

    We copy the file(s) directly rather than using `COPY FROM DATABASE`, which
    ignores foreign-key dependency order and fails on this schema. The canonical
    DB is only read; any WAL is copied alongside it, then merged into the BACKUP
    copy (CHECKPOINT writes only to the backup, never the canonical file). A
    sanity query verifies the snapshot is openable before we trust it.
    """
    ts = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
    out = os.path.join(backups_dir, f"budgeting-tool-{ts}.db")

    shutil.copy2(db_path, out)
    wal = db_path + ".wal"
    if os.path.exists(wal):
        shutil.copy2(wal, out + ".wal")

    con = duckdb.connect(out)
    try:
        con.execute("CHECKPOINT")              # merge WAL into the backup copy
        con.execute("SELECT count(*) FROM categories")  # integrity sanity check
    except Exception:
        con.close()
        for f in (out, out + ".wal"):
            if os.path.exists(f):
                os.remove(f)
        raise
    else:
        con.close()
    return out


def prune(backups_dir, retention):
    """Delete oldest snapshots beyond the retention count. Timestamped names
    sort chronologically, so lexical order is chronological order."""
    snaps = sorted(glob.glob(os.path.join(backups_dir, "budgeting-tool-*.db")))
    for old in snaps[:-retention] if retention > 0 else snaps:
        try:
            os.remove(old)
            print(f"Pruned old backup: {os.path.basename(old)}")
        except OSError as e:
            print(f"Could not prune {old}: {e}")


def sync_to_remote(backups_dir, remote):
    """Best-effort offsite sync. Uses `rclone copy` (never deletes from the
    remote), so Drive keeps full history while local disk is pruned. Any
    failure prints a one-line note and returns — it never raises."""
    rclone = shutil.which("rclone")
    if not rclone:
        print("rclone not found; skipping offsite sync (local snapshot kept).")
        return
    cmd = [rclone, "copy", backups_dir, remote,
           "--transfers=4", "--exclude", HASH_FILE]
    try:
        subprocess.run(cmd, timeout=SYNC_TIMEOUT_SECONDS, check=True,
                       capture_output=True, text=True)
        print(f"Synced backups to {remote}")
    except subprocess.TimeoutExpired:
        print("Offsite sync timed out; local snapshot kept, will retry next exit.")
    except subprocess.CalledProcessError as e:
        detail = (e.stderr or "").strip().splitlines()
        msg = detail[-1] if detail else f"exit code {e.returncode}"
        print(f"Offsite sync failed ({msg}); local snapshot kept, will retry next exit.")


def run_backup(db_path=DB_PATH, backups_dir=BACKUPS_DIR,
               retention=RETENTION, remote=DEFAULT_REMOTE, sync=True):
    """Snapshot (if changed) + prune + best-effort offsite sync."""
    if not os.path.exists(db_path):
        print(f"Backup skipped: database '{db_path}' not found.")
        return

    os.makedirs(backups_dir, exist_ok=True)

    current = file_hash(db_path)
    if current == _read_last_hash(backups_dir):
        print("No changes since last backup; skipping snapshot.")
    else:
        out = snapshot(db_path, backups_dir)
        _write_last_hash(backups_dir, current)
        print(f"Snapshot saved: {out}")
        prune(backups_dir, retention)

    if sync:
        sync_to_remote(backups_dir, remote)


if __name__ == "__main__":
    run_backup()
