#!/usr/bin/env python3
"""
Gerenciador de duplicados com retomada automática.

Recursos:
- Escaneia diretório alvo e registra metadados em SQLite.
- Calcula hash SHA-256 em streaming (baixo uso de RAM).
- Retoma de onde parou (arquivos pendentes continuam depois).
- Marca duplicados preservando 1 arquivo por hash.
- Exporta relatório e remove duplicados com confirmação.
- Interface interativa com feedback colorido.
"""

from __future__ import annotations

import argparse
import hashlib
import html
import json
import os
import shutil
import signal
import sqlite3
import subprocess
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator

try:
    from rich.console import Console
    from rich.panel import Panel
    from rich.progress import BarColumn, Progress, SpinnerColumn, TextColumn
    from rich.table import Table

    RICH_AVAILABLE = True
except Exception:  # noqa: BLE001
    RICH_AVAILABLE = False


DEFAULT_TARGET = Path.cwd()
DEFAULT_STATE_DIR = Path.home() / ".dup_setor_state"
DB_NAME = "state.db"
REPORT_DUP = "duplicados_para_remover.csv"
REPORT_ALL = "todos_documentos.csv"
CHUNK_SIZE = 1024 * 1024


class C:
    RESET = "\033[0m"
    BOLD = "\033[1m"
    RED = "\033[31m"
    GREEN = "\033[32m"
    YELLOW = "\033[33m"
    BLUE = "\033[34m"
    CYAN = "\033[36m"


def color(text: str, c: str) -> str:
    return f"{c}{text}{C.RESET}"


def info(msg: str) -> None:
    if RICH_AVAILABLE:
        Console().print(f"[cyan]{msg}[/cyan]")
    else:
        print(color(msg, C.CYAN))


def ok(msg: str) -> None:
    if RICH_AVAILABLE:
        Console().print(f"[green]{msg}[/green]")
    else:
        print(color(msg, C.GREEN))


def warn(msg: str) -> None:
    if RICH_AVAILABLE:
        Console().print(f"[yellow]{msg}[/yellow]")
    else:
        print(color(msg, C.YELLOW))


def err(msg: str) -> None:
    if RICH_AVAILABLE:
        Console().print(f"[red]{msg}[/red]")
    else:
        print(color(msg, C.RED))


def fmt_bytes(value: int) -> str:
    units = ["B", "KB", "MB", "GB", "TB"]
    v = float(value)
    for unit in units:
        if v < 1024 or unit == units[-1]:
            return f"{v:.2f} {unit}"
        v /= 1024
    return f"{value} B"


def now_ts() -> int:
    return int(time.time())


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        while True:
            chunk = f.read(CHUNK_SIZE)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


@dataclass
class Stats:
    total: int = 0
    indexed_new: int = 0
    indexed_changed: int = 0
    hashed: int = 0
    failed: int = 0


class DuplicateManager:
    def __init__(self, target: Path, state_dir: Path):
        self.target = target
        self.state_dir = state_dir
        self.db_path = state_dir / DB_NAME
        self.stop_requested = False
        self._live_line_active = False
        self._state_disk_low_alerted = False
        self._state_disk_critical_alerted = False
        self._target_disk_high_alerted = False
        self.rich = RICH_AVAILABLE
        self.console = Console() if self.rich else None
        self._progress: Progress | None = None
        self._progress_task_id: int | None = None
        self.state_dir.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(self.db_path)
        self.conn.row_factory = sqlite3.Row
        self._init_db()
        signal.signal(signal.SIGINT, self._request_stop)
        signal.signal(signal.SIGTERM, self._request_stop)

    def _request_stop(self, *_args) -> None:
        self.stop_requested = True
        self._clear_live_line()
        warn("\nParada solicitada. Finalizando etapa atual e salvando estado...")

    def close(self) -> None:
        self.conn.close()

    def _init_db(self) -> None:
        cur = self.conn.cursor()
        cur.execute("PRAGMA journal_mode=WAL;")
        cur.execute("PRAGMA synchronous=NORMAL;")
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS files (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                path TEXT UNIQUE NOT NULL,
                size INTEGER NOT NULL,
                mtime INTEGER NOT NULL,
                hash TEXT,
                hash_status TEXT NOT NULL DEFAULT 'pending',
                last_error TEXT,
                duplicate_of INTEGER,
                seen_at INTEGER NOT NULL
            );
            """
        )
        cur.execute("CREATE INDEX IF NOT EXISTS idx_files_hash ON files(hash);")
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_files_status ON files(hash_status);"
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS meta (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );
            """
        )
        self.conn.commit()

    def _print_live(self, msg: str) -> None:
        if self.rich and self._progress is not None and self._progress_task_id is not None:
            self._progress.update(self._progress_task_id, description=msg)
            return
        self._live_line_active = True
        print(f"\r{color(msg, C.CYAN)}", end="", flush=True)

    def _clear_live_line(self) -> None:
        if self.rich and self._progress is not None:
            return
        if self._live_line_active:
            print()
            self._live_line_active = False

    def _upsert_file(self, path: Path, size: int, mtime: int, ts: int) -> str:
        cur = self.conn.cursor()
        row = cur.execute(
            "SELECT size, mtime FROM files WHERE path = ?;", (str(path),)
        ).fetchone()
        if row is None:
            cur.execute(
                """
                INSERT INTO files(path, size, mtime, hash, hash_status, last_error, duplicate_of, seen_at)
                VALUES (?, ?, ?, NULL, 'pending', NULL, NULL, ?);
                """,
                (str(path), size, mtime, ts),
            )
            return "new"
        if row["size"] != size or row["mtime"] != mtime:
            cur.execute(
                """
                UPDATE files
                SET size = ?, mtime = ?, hash = NULL, hash_status = 'pending',
                    last_error = NULL, duplicate_of = NULL, seen_at = ?
                WHERE path = ?;
                """,
                (size, mtime, ts, str(path)),
            )
            return "changed"
        cur.execute("UPDATE files SET seen_at = ? WHERE path = ?;", (ts, str(path)))
        return "same"

    def _iter_target_files(self) -> Iterator[Path]:
        for root, _dirs, files in os.walk(self.target):
            for name in files:
                yield Path(root) / name

    def index_files(self) -> Stats:
        st = Stats()
        ts = now_ts()
        info(f"Indexando arquivos em: {self.target}")
        if self.rich:
            with Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                console=self.console,
                transient=True,
            ) as progress:
                t_id = progress.add_task("Indexacao em andamento...", total=None)
                with self.conn:
                    for path in self._iter_target_files():
                        if self.stop_requested:
                            break
                        try:
                            stat = path.stat()
                            st.total += 1
                            status = self._upsert_file(
                                path=path,
                                size=stat.st_size,
                                mtime=int(stat.st_mtime),
                                ts=ts,
                            )
                            if status == "new":
                                st.indexed_new += 1
                            elif status == "changed":
                                st.indexed_changed += 1
                            if st.total % 500 == 0:
                                progress.update(
                                    t_id,
                                    description=(
                                        f"Indexados: {st.total} "
                                        f"(novos={st.indexed_new}, alterados={st.indexed_changed}, falhas={st.failed})"
                                    ),
                                )
                        except OSError as ex:
                            st.failed += 1
                            warn(f"Falha ao indexar {path}: {ex}")
                    self.conn.execute(
                        "INSERT INTO meta(key, value) VALUES('last_index_ts', ?) "
                        "ON CONFLICT(key) DO UPDATE SET value=excluded.value;",
                        (str(ts),),
                    )
        else:
            with self.conn:
                for path in self._iter_target_files():
                    if self.stop_requested:
                        break
                    try:
                        stat = path.stat()
                        st.total += 1
                        status = self._upsert_file(
                            path=path,
                            size=stat.st_size,
                            mtime=int(stat.st_mtime),
                            ts=ts,
                        )
                        if status == "new":
                            st.indexed_new += 1
                        elif status == "changed":
                            st.indexed_changed += 1
                        if st.total % 2000 == 0:
                            self._print_live(
                                f"Indexados: {st.total} (novos={st.indexed_new}, alterados={st.indexed_changed}, falhas={st.failed})"
                            )
                    except OSError as ex:
                        st.failed += 1
                        warn(f"Falha ao indexar {path}: {ex}")
                self.conn.execute(
                    "INSERT INTO meta(key, value) VALUES('last_index_ts', ?) "
                    "ON CONFLICT(key) DO UPDATE SET value=excluded.value;",
                    (str(ts),),
                )
            self._clear_live_line()
        ok(
            f"Indexacao concluida: total={st.total}, novos={st.indexed_new}, alterados={st.indexed_changed}, falhas={st.failed}"
        )
        return st

    def hash_pending(self) -> Stats:
        st = Stats()
        cur = self.conn.cursor()
        total_pending = cur.execute(
            "SELECT COUNT(*) AS c FROM files WHERE hash_status = 'pending';"
        ).fetchone()["c"]
        info(f"Arquivos pendentes para hash: {total_pending}")
        start = time.time()
        if self.rich:
            with Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                BarColumn(),
                TextColumn("{task.completed}/{task.total}"),
                console=self.console,
                transient=True,
            ) as progress:
                self._progress = progress
                self._progress_task_id = progress.add_task(
                    "Hash em andamento...", total=max(total_pending, 1)
                )
                while not self.stop_requested:
                    row = cur.execute(
                        """
                        SELECT id, path FROM files
                        WHERE hash_status = 'pending'
                        ORDER BY id
                        LIMIT 1;
                        """
                    ).fetchone()
                    if row is None:
                        break
                    file_id = row["id"]
                    path = Path(row["path"])
                    try:
                        digest = sha256_file(path)
                        with self.conn:
                            self.conn.execute(
                                """
                                UPDATE files
                                SET hash = ?, hash_status = 'done', last_error = NULL
                                WHERE id = ?;
                                """,
                                (digest, file_id),
                            )
                        st.hashed += 1
                    except Exception as ex:  # noqa: BLE001
                        st.failed += 1
                        with self.conn:
                            self.conn.execute(
                                """
                                UPDATE files
                                SET hash_status = 'error', last_error = ?
                                WHERE id = ?;
                                """,
                                (str(ex), file_id),
                            )
                    progress.update(self._progress_task_id, advance=1)
                    if (st.hashed + st.failed) % 100 == 0:
                        elapsed = max(time.time() - start, 0.001)
                        rate = (st.hashed + st.failed) / elapsed
                        self._print_live(
                            f"Hash: {st.hashed + st.failed}/{total_pending} ({rate:.1f} arqs/s), erros={st.failed}"
                        )
                        self._check_disk_pressure()
                self._progress = None
                self._progress_task_id = None
        else:
            while not self.stop_requested:
                row = cur.execute(
                    """
                    SELECT id, path FROM files
                    WHERE hash_status = 'pending'
                    ORDER BY id
                    LIMIT 1;
                    """
                ).fetchone()
                if row is None:
                    break
                file_id = row["id"]
                path = Path(row["path"])
                try:
                    digest = sha256_file(path)
                    with self.conn:
                        self.conn.execute(
                            """
                            UPDATE files
                            SET hash = ?, hash_status = 'done', last_error = NULL
                            WHERE id = ?;
                            """,
                            (digest, file_id),
                        )
                    st.hashed += 1
                except Exception as ex:  # noqa: BLE001
                    st.failed += 1
                    with self.conn:
                        self.conn.execute(
                            """
                            UPDATE files
                            SET hash_status = 'error', last_error = ?
                            WHERE id = ?;
                            """,
                            (str(ex), file_id),
                        )
                if (st.hashed + st.failed) % 100 == 0:
                    elapsed = max(time.time() - start, 0.001)
                    rate = (st.hashed + st.failed) / elapsed
                    self._print_live(
                        f"Hash progresso: {st.hashed + st.failed}/{total_pending} ({rate:.1f} arqs/s), erros={st.failed}"
                    )
                    self._check_disk_pressure()
        self._clear_live_line()
        ok(f"Hash concluido: processados={st.hashed}, erros={st.failed}")
        return st

    def _check_disk_pressure(self) -> None:
        try:
            usage_state = shutil.disk_usage(self.state_dir)
            usage_target = shutil.disk_usage(self.target)
            free_state_ratio = usage_state.free / usage_state.total
            target_used_ratio = (usage_target.total - usage_target.free) / usage_target.total
            if free_state_ratio < 0.08:
                if not self._state_disk_low_alerted:
                    self._clear_live_line()
                    warn(
                        "Espaco livre da unidade de estado abaixo de 8%. Exporte e limpe relatorios antigos."
                    )
                    self._state_disk_low_alerted = True
            else:
                self._state_disk_low_alerted = False
            if free_state_ratio < 0.03 and not self._state_disk_critical_alerted:
                self._clear_live_line()
                err("Estado critico: unidade de estado abaixo de 3% livre.")
                self._state_disk_critical_alerted = True
            elif free_state_ratio >= 0.03:
                self._state_disk_critical_alerted = False

            if target_used_ratio >= 0.90 and not self._target_disk_high_alerted:
                self._clear_live_line()
                warn("Atencao: disco alvo acima de 90% de uso.")
                self._target_disk_high_alerted = True
            elif target_used_ratio < 0.90:
                self._target_disk_high_alerted = False
        except OSError:
            pass

    def _fetch_counts(self) -> dict[str, int]:
        cur = self.conn.cursor()
        return {
            "total": cur.execute("SELECT COUNT(*) AS c FROM files;").fetchone()["c"],
            "done": cur.execute(
                "SELECT COUNT(*) AS c FROM files WHERE hash_status='done';"
            ).fetchone()["c"],
            "pending": cur.execute(
                "SELECT COUNT(*) AS c FROM files WHERE hash_status='pending';"
            ).fetchone()["c"],
            "errors": cur.execute(
                "SELECT COUNT(*) AS c FROM files WHERE hash_status='error';"
            ).fetchone()["c"],
            "dups": cur.execute(
                "SELECT COUNT(*) AS c FROM files WHERE duplicate_of IS NOT NULL;"
            ).fetchone()["c"],
        }

    def mark_duplicates(self) -> int:
        info("Recalculando marcacao de duplicados...")
        with self.conn:
            self.conn.execute("UPDATE files SET duplicate_of = NULL;")
            groups = self.conn.execute(
                """
                SELECT hash
                FROM files
                WHERE hash_status='done' AND hash IS NOT NULL
                GROUP BY hash
                HAVING COUNT(*) > 1;
                """
            ).fetchall()
            total = 0
            for g in groups:
                h = g["hash"]
                members = self.conn.execute(
                    """
                    SELECT id
                    FROM files
                    WHERE hash = ?
                    ORDER BY mtime ASC, id ASC;
                    """,
                    (h,),
                ).fetchall()
                keeper_id = members[0]["id"]
                dup_ids = [m["id"] for m in members[1:]]
                for dup_id in dup_ids:
                    self.conn.execute(
                        "UPDATE files SET duplicate_of = ? WHERE id = ?;",
                        (keeper_id, dup_id),
                    )
                total += len(dup_ids)
        ok(f"Duplicados marcados: {total}")
        return total

    def show_summary(self) -> None:
        counts = self._fetch_counts()
        db_size = self.db_path.stat().st_size if self.db_path.exists() else 0
        if self.rich:
            table = Table(title="Resumo detalhado", show_header=True, header_style="bold cyan")
            table.add_column("Metrica")
            table.add_column("Valor", justify="right")
            table.add_row("Arquivos registrados", str(counts["total"]))
            table.add_row("Hash processado", str(counts["done"]))
            table.add_row("Aguardando hash", str(counts["pending"]))
            table.add_row("Arquivos com erro", str(counts["errors"]))
            table.add_row("Duplicados marcados", str(counts["dups"]))
            table.add_row("Banco SQLite", f"{self.db_path} ({fmt_bytes(db_size)})")
            self.console.print(table)
            self._print_disk_summary()
        else:
            print()
            print(color("===== RESUMO =====", C.BOLD))
            print(f"Arquivos cadastrados: {counts['total']}")
            print(f"Hash concluido:       {counts['done']}")
            print(f"Pendentes:            {counts['pending']}")
            print(f"Com erro:             {counts['errors']}")
            print(f"Duplicados:           {counts['dups']}")
            print(f"Banco de estado:      {self.db_path} ({fmt_bytes(db_size)})")
            self._print_disk_summary()
            print()

    def show_startup_status(self) -> None:
        counts = self._fetch_counts()
        resumed = "dados anteriores encontrados" if counts["total"] else "sem dados anteriores"
        if self.rich:
            self.console.print(
                Panel(
                    "[bold cyan]JACK ZACARIAS SECURITY LAB[/bold cyan]\n"
                    "[bold]Duplicate Setor Manager[/bold]\n"
                    "Resilient Duplicate Scanner • DFIR • Data Safety",
                    border_style="cyan",
                )
            )
            info_table = Table(show_header=False, box=None)
            info_table.add_column("Campo", style="cyan")
            info_table.add_column("Valor")
            info_table.add_row("Alvo", str(self.target))
            info_table.add_row("Estado", str(self.state_dir))
            info_table.add_row("Banco SQLite", str(self.db_path))
            info_table.add_row("Relatorio geral CSV", str(self.state_dir / REPORT_ALL))
            info_table.add_row("Relatorio duplicados CSV", str(self.state_dir / REPORT_DUP))
            info_table.add_row("Status de retomada", resumed)
            self.console.print(Panel(info_table, title="Informacoes", border_style="blue"))

            status_table = Table(show_header=False, box=None)
            status_table.add_column("Metrica", style="cyan")
            status_table.add_column("Valor", justify="right")
            status_table.add_row("Arquivos registrados", str(counts["total"]))
            status_table.add_row("Hash processado", str(counts["done"]))
            status_table.add_row("Aguardando hash", str(counts["pending"]))
            status_table.add_row("Arquivos com erro", str(counts["errors"]))
            status_table.add_row("Duplicados marcados", str(counts["dups"]))
            self.console.print(Panel(status_table, title="Status da Operacao", border_style="green"))
            self._print_disk_summary()
        else:
            print(color("\n===== ESTADO SALVO =====", C.BOLD))
            print(f"Banco SQLite:         {self.db_path}")
            print(f"Relatorio geral CSV:  {self.state_dir / REPORT_ALL}")
            print(f"Relatorio duplicados: {self.state_dir / REPORT_DUP}")
            print(f"Retomada:             {resumed}.")
            print(f"Ja processado (hash): {counts['done']}")
            print(f"Aguardando hash:      {counts['pending']}")
            print(f"Com erro:             {counts['errors']}")
            print(f"Duplicados marcados:  {counts['dups']}")
            print(f"Arquivos registrados: {counts['total']}")
            self._print_disk_summary()
            print()

    def _print_disk_summary(self) -> None:
        try:
            t = shutil.disk_usage(self.target)
            s = shutil.disk_usage(self.state_dir)
            t_used = t.total - t.free
            target_used_pct = (t_used / t.total) * 100
            target_free_pct = (t.free / t.total) * 100
            state_free_pct = (s.free / s.total) * 100
            if self.rich:
                bars = Table(show_header=False, box=None)
                bars.add_column("Item", style="cyan")
                bars.add_column("Percentual")
                bars.add_row(
                    "Disco alvo usado",
                    f"{target_used_pct:.1f}% ({fmt_bytes(t_used)})",
                )
                bars.add_row(
                    "Disco alvo livre",
                    f"{target_free_pct:.1f}% ({fmt_bytes(t.free)})",
                )
                bars.add_row(
                    "Disco estado livre",
                    f"{state_free_pct:.1f}% ({fmt_bytes(s.free)})",
                )
                self.console.print(Panel(bars, title="Discos", border_style="magenta"))
            else:
                print(
                    f"Disco alvo usado:     {fmt_bytes(t_used)} ({target_used_pct:.1f}%)"
                )
                print(
                    f"Disco alvo livre:     {fmt_bytes(t.free)} ({target_free_pct:.1f}%)"
                )
                print(
                    f"Disco estado livre:   {fmt_bytes(s.free)} ({state_free_pct:.1f}%)"
                )
        except OSError:
            warn("Nao foi possivel ler uso de disco.")

    def _run_cmd(self, cmd: list[str]) -> tuple[bool, str]:
        try:
            cp = subprocess.run(
                cmd, check=False, capture_output=True, text=True, timeout=20
            )
            if cp.returncode == 0:
                return True, cp.stdout.strip()
            return False, (cp.stderr or cp.stdout).strip()
        except Exception as ex:  # noqa: BLE001
            return False, str(ex)

    def _find_mountpoint_for_target(self) -> str | None:
        target = str(self.target.resolve())
        best: str | None = None
        try:
            with open("/proc/mounts", "r", encoding="utf-8") as f:
                for line in f:
                    parts = line.split()
                    if len(parts) < 2:
                        continue
                    mnt = parts[1].replace("\\040", " ")
                    if target == mnt or target.startswith(mnt.rstrip("/") + "/"):
                        if best is None or len(mnt) > len(best):
                            best = mnt
        except OSError:
            return None
        return best

    def _find_lsblk_node_by_mountpoint(
        self, node: dict, mountpoint: str
    ) -> tuple[dict | None, dict | None]:
        if node.get("mountpoint") == mountpoint:
            parent = node
            while parent.get("pkname"):
                parent_name = parent.get("pkname")
                parent = self._find_lsblk_node_by_name(self._lsblk_tree, parent_name) or parent
                if parent.get("name") == parent_name and not parent.get("pkname"):
                    break
            return node, parent
        for ch in node.get("children", []) or []:
            found, parent = self._find_lsblk_node_by_mountpoint(ch, mountpoint)
            if found:
                return found, parent
        return None, None

    def _find_lsblk_node_by_name(self, nodes: list[dict], name: str) -> dict | None:
        for n in nodes:
            if n.get("name") == name:
                return n
            children = n.get("children", []) or []
            found = self._find_lsblk_node_by_name(children, name)
            if found:
                return found
        return None

    def _smart_value(self, smart_json: dict, attr_names: list[str]) -> str | None:
        table = smart_json.get("ata_smart_attributes", {}).get("table", [])
        for row in table:
            nm = str(row.get("name", "")).strip()
            if nm in attr_names:
                raw = row.get("raw", {})
                value = raw.get("value")
                if value is not None:
                    return str(value)
        nvme_log = smart_json.get("nvme_smart_health_information_log", {})
        for key in attr_names:
            if key in nvme_log:
                return str(nvme_log.get(key))
        return None

    def show_target_disk_health(self) -> None:
        print(color("\n===== SAUDE DO DISCO ALVO =====", C.BOLD))
        mountpoint = self._find_mountpoint_for_target()
        if not mountpoint:
            warn("Nao foi possivel detectar o ponto de montagem do alvo.")
            return
        print(f"Ponto de montagem:    {mountpoint}")

        ok_lsblk, out_lsblk = self._run_cmd(
            [
                "lsblk",
                "-J",
                "-o",
                "NAME,PATH,TYPE,SIZE,MODEL,SERIAL,ROTA,TRAN,MOUNTPOINT,PKNAME",
            ]
        )
        if not ok_lsblk:
            warn(f"Falha ao obter dados do disco (lsblk): {out_lsblk}")
            return
        try:
            js = json.loads(out_lsblk)
            self._lsblk_tree = js.get("blockdevices", [])
        except json.JSONDecodeError:
            warn("Resposta invalida do lsblk.")
            return

        part = None
        disk = None
        for n in self._lsblk_tree:
            part, disk = self._find_lsblk_node_by_mountpoint(n, mountpoint)
            if part:
                break
        if not part:
            warn("Nao foi possivel mapear particao/disco do alvo.")
            return
        if not disk:
            disk = part

        disk_path = disk.get("path") or f"/dev/{disk.get('name')}"
        print(f"Dispositivo:          {disk_path}")
        print(f"Modelo:               {disk.get('model') or 'N/D'}")
        print(f"Serial:               {disk.get('serial') or 'N/D'}")
        print(f"Tamanho:              {disk.get('size') or 'N/D'}")
        rota = disk.get("rota")
        print(f"Tipo fisico:          {'HDD (rotacional)' if str(rota) == '1' else 'SSD/NVMe'}")
        print(f"Transporte:           {disk.get('tran') or 'N/D'}")

        smartctl_path = shutil.which("smartctl")
        if not smartctl_path:
            warn("smartctl nao encontrado. Instale: sudo apt install smartmontools")
            return

        ok_smart, out_smart = self._run_cmd([smartctl_path, "-j", "-H", "-A", disk_path])
        if not ok_smart:
            warn(
                "Nao foi possivel ler SMART. Tente com permissao elevada: "
                f"sudo {smartctl_path} -H -A {disk_path}"
            )
            return
        try:
            smart = json.loads(out_smart)
        except json.JSONDecodeError:
            warn("Resposta SMART invalida.")
            return

        health = (
            smart.get("smart_status", {}).get("passed")
            if isinstance(smart.get("smart_status"), dict)
            else None
        )
        print(f"SMART geral:          {'APROVADO' if health else 'ATENCAO/INDEFINIDO'}")

        poh = self._smart_value(smart, ["Power_On_Hours", "power_on_hours"])
        starts = self._smart_value(
            smart, ["Start_Stop_Count", "Power_Cycle_Count", "power_cycles"]
        )
        pct_used = self._smart_value(smart, ["percentage_used"])
        realloc = self._smart_value(smart, ["Reallocated_Sector_Ct"])
        pending = self._smart_value(smart, ["Current_Pending_Sector"])
        offline_unc = self._smart_value(smart, ["Offline_Uncorrectable"])
        media_err = self._smart_value(
            smart, ["media_errors", "Media_and_Data_Integrity_Errors"]
        )

        print(f"Horas ligado:         {poh or 'N/D'}")
        print(f"Ciclos/partidas:      {starts or 'N/D'}")
        print(f"Desgaste SSD/NVMe:    {pct_used + '%' if pct_used else 'N/D'}")
        print(f"Setores realocados:   {realloc or 'N/D'}")
        print(f"Setores pendentes:    {pending or 'N/D'}")
        print(f"Erros nao corrigidos: {offline_unc or media_err or 'N/D'}")

        risks: list[str] = []
        if health is False:
            risks.append("SMART reprovado")
        if realloc and realloc.isdigit() and int(realloc) > 0:
            risks.append("setores realocados > 0")
        if pending and pending.isdigit() and int(pending) > 0:
            risks.append("setores pendentes > 0")
        if offline_unc and offline_unc.isdigit() and int(offline_unc) > 0:
            risks.append("erros nao corrigidos > 0")
        if media_err and media_err.isdigit() and int(media_err) > 0:
            risks.append("erros de midia > 0")
        if pct_used and pct_used.isdigit() and int(pct_used) >= 80:
            risks.append("desgaste SSD/NVMe >= 80%")

        if risks:
            warn("Risco detectado: " + "; ".join(risks))
            warn("Recomendacao: preparar substituicao e garantir backup atualizado.")
        else:
            ok("Nenhum indicador critico detectado nas metricas disponiveis.")

    def export_reports(self) -> tuple[Path, Path]:
        all_path = self.state_dir / REPORT_ALL
        dup_path = self.state_dir / REPORT_DUP
        info(f"Exportando relatórios para: {self.state_dir}")
        with self.conn:
            rows_all = self.conn.execute(
                """
                SELECT path, size, mtime, hash, hash_status, duplicate_of
                FROM files
                ORDER BY path;
                """
            ).fetchall()
            rows_dup = self.conn.execute(
                """
                SELECT f.path AS duplicate_path, o.path AS original_path, f.hash, f.size
                FROM files f
                JOIN files o ON f.duplicate_of = o.id
                ORDER BY o.path, f.path;
                """
            ).fetchall()
        with all_path.open("w", encoding="utf-8") as f:
            f.write("path,size,mtime,hash,hash_status,duplicate_of\n")
            for r in rows_all:
                f.write(
                    f"\"{r['path'].replace('\"', '\"\"')}\",{r['size']},{r['mtime']},"
                    f"\"{(r['hash'] or '').replace('\"', '\"\"')}\",{r['hash_status']},{r['duplicate_of'] or ''}\n"
                )
        with dup_path.open("w", encoding="utf-8") as f:
            f.write("duplicate_path,original_path,hash,size\n")
            for r in rows_dup:
                f.write(
                    f"\"{r['duplicate_path'].replace('\"', '\"\"')}\","
                    f"\"{r['original_path'].replace('\"', '\"\"')}\","
                    f"\"{(r['hash'] or '').replace('\"', '\"\"')}\",{r['size']}\n"
                )
        ok(f"Relatórios exportados: {all_path} | {dup_path}")
        return all_path, dup_path

    def export_html_report(self) -> Path:
        html_path = self.state_dir / "relatorio_duplicados.html"
        counts = self._fetch_counts()
        rows = self.conn.execute(
            """
            SELECT o.path AS original_path, f.path AS duplicate_path, f.size AS size
            FROM files f
            JOIN files o ON f.duplicate_of = o.id
            ORDER BY o.path, f.path;
            """
        ).fetchall()
        recoverable = sum(int(r["size"]) for r in rows)
        grouped: dict[str, list[sqlite3.Row]] = {}
        for r in rows:
            grouped.setdefault(r["original_path"], []).append(r)

        with html_path.open("w", encoding="utf-8") as f:
            f.write("<!doctype html><html><head><meta charset='utf-8'>")
            f.write("<title>Relatorio de Duplicados</title>")
            f.write("<style>body{font-family:Arial,sans-serif;padding:20px;}table{border-collapse:collapse;width:100%;}th,td{border:1px solid #ddd;padding:8px;}th{background:#f3f3f3;}h2{margin-top:24px;}</style>")
            f.write("</head><body>")
            f.write("<h1>Duplicate Setor Manager - Relatorio</h1>")
            f.write(f"<p><b>Arquivos registrados:</b> {counts['total']}<br>")
            f.write(f"<b>Hash processado:</b> {counts['done']}<br>")
            f.write(f"<b>Duplicados marcados:</b> {counts['dups']}<br>")
            f.write(f"<b>Espaco recuperavel:</b> {html.escape(fmt_bytes(recoverable))}</p>")
            for original, items in grouped.items():
                f.write(f"<h2>Original: {html.escape(original)}</h2>")
                f.write("<table><tr><th>Duplicado</th><th>Tamanho</th></tr>")
                for item in items:
                    f.write(
                        "<tr><td>"
                        + html.escape(item["duplicate_path"])
                        + "</td><td>"
                        + html.escape(fmt_bytes(int(item["size"])))
                        + "</td></tr>"
                    )
                f.write("</table>")
            f.write("</body></html>")
        ok(f"Relatorio HTML exportado: {html_path}")
        return html_path

    def simulate_delete_marked_duplicates(self) -> None:
        row = self.conn.execute(
            """
            SELECT COUNT(*) AS c, COALESCE(SUM(size),0) AS total_size
            FROM files
            WHERE duplicate_of IS NOT NULL;
            """
        ).fetchone()
        count = int(row["c"])
        total_size = int(row["total_size"])
        info(
            f"DRY-RUN: seriam removidos {count} arquivos, liberando {fmt_bytes(total_size)}."
        )

    def show_top_waste(self, limit: int = 10) -> None:
        rows = self.conn.execute(
            """
            SELECT o.path AS original_path,
                   COUNT(f.id) AS copies,
                   COALESCE(SUM(f.size),0) AS recoverable
            FROM files f
            JOIN files o ON f.duplicate_of = o.id
            GROUP BY o.id
            ORDER BY recoverable DESC
            LIMIT ?;
            """,
            (limit,),
        ).fetchall()
        if not rows:
            warn("Nao ha grupos de duplicados para exibir.")
            return
        if self.rich:
            table = Table(title="Top desperdicios por espaco recuperavel")
            table.add_column("Original preservado", style="cyan")
            table.add_column("Copias")
            table.add_column("Recuperavel", justify="right")
            for r in rows:
                table.add_row(r["original_path"], str(r["copies"]), fmt_bytes(int(r["recoverable"])))
            self.console.print(table)
        else:
            print(color("Top desperdicios:", C.BOLD))
            for r in rows:
                print(
                    f"- {r['original_path']} | copias={r['copies']} | recuperavel={fmt_bytes(int(r['recoverable']))}"
                )

    def delete_marked_duplicates(self, batch: int = 100) -> None:
        cur = self.conn.cursor()
        total = cur.execute(
            "SELECT COUNT(*) AS c FROM files WHERE duplicate_of IS NOT NULL;"
        ).fetchone()["c"]
        if total == 0:
            warn("Nao ha duplicados marcados para remover.")
            return
        warn(
            f"ATENCAO: {total} arquivos marcados para exclusao. "
            "Recomendado exportar relatorio antes."
        )
        confirm = input("Digite EXCLUIR para confirmar: ").strip()
        if confirm != "EXCLUIR":
            warn("Operacao cancelada.")
            return
        removed = 0
        failed = 0
        bytes_removed = 0
        while not self.stop_requested:
            rows = cur.execute(
                """
                SELECT id, path, size
                FROM files
                WHERE duplicate_of IS NOT NULL
                ORDER BY id
                LIMIT ?;
                """,
                (batch,),
            ).fetchall()
            if not rows:
                break
            for r in rows:
                path = Path(r["path"])
                file_id = r["id"]
                size = r["size"]
                try:
                    if path.exists():
                        path.unlink()
                        bytes_removed += size
                    with self.conn:
                        self.conn.execute("DELETE FROM files WHERE id = ?;", (file_id,))
                    removed += 1
                except Exception as ex:  # noqa: BLE001
                    failed += 1
                    with self.conn:
                        self.conn.execute(
                            "UPDATE files SET last_error = ? WHERE id = ?;",
                            (f"delete_error: {ex}", file_id),
                        )
            info(
                f"Remocao em progresso: removidos={removed}, falhas={failed}, liberado={fmt_bytes(bytes_removed)}"
            )
        ok(
            f"Remocao finalizada: removidos={removed}, falhas={failed}, liberado={fmt_bytes(bytes_removed)}"
        )

    def retry_errors(self) -> None:
        with self.conn:
            self.conn.execute(
                """
                UPDATE files
                SET hash_status='pending', last_error=NULL
                WHERE hash_status='error';
                """
            )
        ok("Arquivos com erro voltaram para fila pendente.")

    def run_scan_cycle(self) -> None:
        self.stop_requested = False
        self.index_files()
        if self.stop_requested:
            warn("Escaneamento interrompido na indexacao.")
            return
        self.hash_pending()
        if self.stop_requested:
            warn("Escaneamento interrompido no hash.")
            return
        self.mark_duplicates()
        self.show_summary()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Gerenciador de duplicados com retomada e baixa memoria."
    )
    parser.add_argument(
        "--target",
        default=str(DEFAULT_TARGET),
        help=f"Diretorio para escanear (padrao: {DEFAULT_TARGET})",
    )
    parser.add_argument(
        "--state-dir",
        default=str(DEFAULT_STATE_DIR),
        help=f"Diretorio de estado/relatorios (padrao: {DEFAULT_STATE_DIR})",
    )
    parser.add_argument(
        "--run-once",
        action="store_true",
        help="Executa ciclo completo uma vez e encerra.",
    )
    return parser.parse_args()


def print_menu(manager: DuplicateManager) -> None:
    lines = [
        "[1] Escanear / retomar agora",
        "[2] Mostrar resumo detalhado",
        "[3] Recalcular duplicados",
        "[4] Exportar relatorios CSV / HTML",
        "[5] Simular remocao segura",
        "[6] Remover duplicados marcados",
        "[7] Reprocessar arquivos com erro",
        "[8] Diagnostico do disco alvo",
        "[9] Ver maiores desperdicios de espaco",
        "[0] Sair",
    ]
    if manager.rich:
        manager.console.print(Panel("\n".join(lines), title="Menu Principal", border_style="cyan"))
    else:
        print(color("\n===== MENU =====", C.BOLD))
        for line in lines:
            print(line)


def main() -> int:
    args = parse_args()
    target = Path(args.target).expanduser().resolve()
    state_dir = Path(args.state_dir).expanduser().resolve()
    if not target.exists() or not target.is_dir():
        err(f"Diretorio alvo invalido: {target}")
        return 1
    manager = DuplicateManager(target=target, state_dir=state_dir)
    try:
        if not manager.rich:
            warn("Biblioteca 'rich' nao encontrada. Usando interface simples.")
        manager.show_startup_status()
        if manager.rich:
            manager.console.print(
                "[dim]Author: Jackson Zacarias[/dim]\n"
                "[dim]Built by Jackson Zacarias • Cybersecurity | DFIR | Automation[/dim]\n"
                "[dim]GitHub: github.com/jacksonzacarias[/dim]"
            )
        if args.run_once:
            manager.run_scan_cycle()
            return 0
        while True:
            print_menu(manager)
            choice = input(color("Escolha: ", C.BLUE)).strip()
            if choice == "1":
                manager.run_scan_cycle()
            elif choice == "2":
                manager.show_summary()
            elif choice == "3":
                manager.mark_duplicates()
            elif choice == "4":
                manager.export_reports()
                manager.export_html_report()
            elif choice == "5":
                manager.simulate_delete_marked_duplicates()
            elif choice == "6":
                manager.delete_marked_duplicates()
            elif choice == "7":
                manager.retry_errors()
            elif choice == "8":
                manager.show_target_disk_health()
            elif choice == "9":
                manager.show_top_waste()
            elif choice == "0":
                if manager.rich:
                    manager.console.print(
                        "\n[dim]Built by Jackson Zacarias • Cybersecurity | DFIR | Automation[/dim]\n"
                        "[dim]github.com/jacksonzacarias[/dim]"
                    )
                print("Saindo.")
                break
            else:
                warn("Opcao invalida.")
    finally:
        manager.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
