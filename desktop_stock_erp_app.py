#!/usr/bin/env python3
"""Desktop integration app for Firebird stock synchronization."""

from __future__ import annotations

import csv
import json
import os
import queue
import threading
import time
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Callable
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

import tkinter as tk
from tkinter import filedialog, messagebox, ttk

try:
    import requests
except Exception as exc:  # pragma: no cover - import guard
    requests = None
    REQUESTS_IMPORT_ERROR = exc
else:
    REQUESTS_IMPORT_ERROR = None

try:
    from firebird.driver import connect as fb_connect
    from firebird.driver import driver_config as fb_driver_config
except Exception as exc:  # pragma: no cover - import guard
    fb_connect = None
    fb_driver_config = None
    FIREBIRD_IMPORT_ERROR = exc
else:
    FIREBIRD_IMPORT_ERROR = None

try:
    from produce_pachet_service import FirebirdConnectionSettings, producePachet
except Exception as exc:  # pragma: no cover - import guard
    FirebirdConnectionSettings = None
    producePachet = None
    PRODUCE_PACHET_IMPORT_ERROR = exc
else:
    PRODUCE_PACHET_IMPORT_ERROR = None

CONFIG_PATH = Path("config.json")


def to_int(value: Any, default: int, minimum: int) -> int:
    """Convert value to int with minimum bound."""
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return default
    if parsed < minimum:
        return default
    return parsed


def to_bool(value: Any, default: bool) -> bool:
    """Convert arbitrary input to bool in a predictable way."""
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"1", "true", "yes", "on"}:
            return True
        if normalized in {"0", "false", "no", "off"}:
            return False
    return default


@dataclass
class AppConfig:
    db_path: str = ""
    db_host: str = ""
    db_port: int = 3050
    db_user: str = "SYSDBA"
    db_password: str = "masterkey"
    db_charset: str = "UTF8"
    fb_client_library_path: str = ""
    enable_sync_job: bool = True
    sync_interval_seconds: int = 120
    pachet_import_api_url: str = ""
    sync_api_token: str = ""
    enable_export_job: bool = True
    export_interval_seconds: int = 300
    stock_select_sql: str = "SELECT SKU, QTY FROM STOCKS"
    upload_url: str = ""
    upload_field_name: str = "file"
    upload_api_token: str = ""
    upload_token_query_param: str = ""
    upload_headers_json: str = ""
    upload_user_agent: str = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) DesktopStockErpIntegration/1.0"
    csv_directory: str = "exports"
    audit_log_directory: str = "audit_logs"
    http_timeout_seconds: int = 30
    verify_ssl: bool = True
    extra_upload_fields_json: str = ""

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "AppConfig":
        return cls(
            db_path=str(data.get("db_path", "")),
            db_host=str(data.get("db_host", "")),
            db_port=to_int(data.get("db_port"), default=3050, minimum=1),
            db_user=str(data.get("db_user", "SYSDBA")),
            db_password=str(data.get("db_password", "masterkey")),
            db_charset=str(data.get("db_charset", "UTF8")),
            fb_client_library_path=str(data.get("fb_client_library_path", "")),
            enable_sync_job=to_bool(data.get("enable_sync_job"), default=True),
            sync_interval_seconds=to_int(
                data.get("sync_interval_seconds"),
                default=120,
                minimum=5,
            ),
            pachet_import_api_url=str(data.get("pachet_import_api_url", "")),
            sync_api_token=str(data.get("sync_api_token", "")),
            enable_export_job=to_bool(data.get("enable_export_job"), default=True),
            export_interval_seconds=to_int(
                data.get("export_interval_seconds"),
                default=300,
                minimum=10,
            ),
            stock_select_sql=str(data.get("stock_select_sql", "SELECT SKU, QTY FROM STOCKS")),
            upload_url=str(data.get("upload_url", "")),
            upload_field_name=str(data.get("upload_field_name", "file")),
            upload_api_token=str(data.get("upload_api_token", "")),
            upload_token_query_param=str(data.get("upload_token_query_param", "")),
            upload_headers_json=str(data.get("upload_headers_json", "")),
            upload_user_agent=str(
                data.get(
                    "upload_user_agent",
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) DesktopStockErpIntegration/1.0",
                )
            ),
            csv_directory=str(data.get("csv_directory", "exports")),
            audit_log_directory=str(data.get("audit_log_directory", "audit_logs")),
            http_timeout_seconds=to_int(
                data.get("http_timeout_seconds"),
                default=30,
                minimum=5,
            ),
            verify_ssl=to_bool(data.get("verify_ssl"), default=True),
            extra_upload_fields_json=str(data.get("extra_upload_fields_json", "")),
        )

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def load_config(config_path: Path) -> AppConfig:
    if not config_path.exists():
        return AppConfig()
    try:
        raw = json.loads(config_path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return AppConfig()
    if not isinstance(raw, dict):
        return AppConfig()
    return AppConfig.from_dict(raw)


def save_config(config_path: Path, config: AppConfig) -> None:
    config_path.write_text(
        json.dumps(config.to_dict(), indent=2, ensure_ascii=True),
        encoding="utf-8",
    )


class IntegrationService:
    """Executes Firebird queries and API calls."""

    def __init__(self, log_fn: Callable[[str], None]) -> None:
        self.log = log_fn
        self._dll_dir_handles: list[Any] = []
        self._sensitive_query_keys = {
            "token",
            "access_token",
            "api_key",
            "apikey",
            "key",
            "password",
        }

    def _ensure_dependencies(self) -> None:
        if requests is None:
            raise RuntimeError(f"Missing dependency 'requests': {REQUESTS_IMPORT_ERROR}")
        if fb_connect is None:
            raise RuntimeError(f"Missing dependency 'firebird-driver': {FIREBIRD_IMPORT_ERROR}")

    @staticmethod
    def _build_db_target(config: AppConfig) -> str:
        db_path = config.db_path.strip()
        if not db_path:
            raise ValueError("Database file path is empty.")

        host = config.db_host.strip()
        if host:
            return f"{host}/{config.db_port}:{db_path}"
        return db_path

    def _configure_client_library(self, config: AppConfig) -> None:
        library_raw = config.fb_client_library_path.strip()
        if not library_raw:
            return

        library_path = Path(library_raw).expanduser()
        if not library_path.exists():
            raise ValueError(f"Firebird client library file was not found: {library_path}")

        if fb_driver_config is not None:
            fb_driver_config.fb_client_library.value = str(library_path)

        # On Windows make sure folder containing fbclient.dll is discoverable by loader.
        if os.name == "nt" and hasattr(os, "add_dll_directory"):
            handle = os.add_dll_directory(str(library_path.parent))
            self._dll_dir_handles.append(handle)

    def _connect(self, config: AppConfig):
        target = self._build_db_target(config)
        self.log(f"Connecting to Firebird at: {target}")
        self._configure_client_library(config)
        try:
            return fb_connect(
                database=target,
                user=config.db_user.strip(),
                password=config.db_password,
                charset=config.db_charset.strip() or "UTF8",
            )
        except Exception as exc:  # pylint: disable=broad-except
            message = str(exc)
            lowered = message.lower()
            if "client library" in lowered or "fbclient" in lowered:
                raise RuntimeError(
                    "Firebird client library is missing. "
                    "Set 'Firebird client library (fbclient.dll)' in Firebird tab "
                    "or install Firebird client and add it to PATH."
                ) from exc
            raise

    def _fetch_json_from_api(
        self,
        *,
        url: str,
        token: str,
        config: AppConfig,
        job_name: str,
    ) -> Any:
        headers = {"Accept": "application/json"}
        if token:
            headers["Authorization"] = f"Bearer {token}"

        self.log(f"Fetching {job_name} payload from API: {self._sanitize_url_for_log(url)}")
        response = requests.get(
            url,
            headers=headers,
            timeout=config.http_timeout_seconds,
            verify=config.verify_ssl,
        )
        response.raise_for_status()
        return response.json()

    @staticmethod
    def _extract_pachet_requests(payload: Any) -> list[dict[str, Any]]:
        if isinstance(payload, dict) and isinstance(payload.get("pachet"), dict) and isinstance(payload.get("produse"), list):
            source = [payload]
        elif isinstance(payload, list):
            source = payload
        elif isinstance(payload, dict):
            source = []
            for key in ("pachete", "items", "data", "results"):
                candidate = payload.get(key)
                if isinstance(candidate, list):
                    source = candidate
                    break
        else:
            source = []

        requests_payloads: list[dict[str, Any]] = []
        for item in source:
            if not isinstance(item, dict):
                continue
            pachet = item.get("pachet")
            produse = item.get("produse")
            if not isinstance(pachet, dict) or not isinstance(produse, list):
                continue
            status = str(pachet.get("status", "")).strip().lower()
            if status and status != "pending":
                continue
            requests_payloads.append(item)
        return requests_payloads

    @staticmethod
    def _build_produce_pachet_db_settings(config: AppConfig):
        if FirebirdConnectionSettings is None:
            raise RuntimeError(
                "produce_pachet_service import failed: "
                f"{PRODUCE_PACHET_IMPORT_ERROR}"
            )
        return FirebirdConnectionSettings(
            database_path=config.db_path.strip(),
            host=config.db_host.strip(),
            port=config.db_port,
            user=config.db_user.strip(),
            password=config.db_password,
            charset=config.db_charset.strip() or "UTF8",
            fb_client_library_path=config.fb_client_library_path.strip(),
        )

    def _run_pachet_import_sync(self, config: AppConfig) -> int:
        url = config.pachet_import_api_url.strip()
        if not url:
            return 0
        if producePachet is None:
            raise RuntimeError(
                f"produce_pachet_service import failed: {PRODUCE_PACHET_IMPORT_ERROR}"
            )

        self.log("Import Pachete Saga: starting fetch from API.")
        payload = self._fetch_json_from_api(
            url=url,
            token=config.sync_api_token.strip(),
            config=config,
            job_name="Import Pachete Saga",
        )
        items = self._extract_pachet_requests(payload)
        self.log(f"Import Pachete Saga: API returned {len(items)} pending request(s).")
        if not items:
            self.log("Import Pachete Saga: no pending items to process.")
            return 0

        db_settings = self._build_produce_pachet_db_settings(config)
        success_count = 0
        error_messages: list[str] = []

        for index, item in enumerate(items, start=1):
            pachet_data = item.get("pachet") if isinstance(item, dict) else {}
            id_doc = str((pachet_data or {}).get("id_doc", "")).strip() or "?"
            denumire = str((pachet_data or {}).get("denumire", "")).strip() or "?"
            self.log(
                "Import Pachete Saga: processing "
                f"#{index}/{len(items)} (id_doc={id_doc}, denumire={denumire})"
            )
            try:
                result = producePachet(item, db_settings)
                success_count += 1
                self.log(
                    "Import Pachete Saga: success "
                    f"#{index}: codPachet={result.get('codPachet')}, "
                    f"nrDoc={result.get('nrDoc')}, idDoc={result.get('idDoc')}"
                )
            except Exception as exc:  # pylint: disable=broad-except
                message = f"Import Pachete Saga: failed #{index}: {exc}"
                error_messages.append(message)
                self.log(message)

        self.log(
            "Import Pachete Saga: finished. "
            f"success={success_count}, failed={len(error_messages)}."
        )
        if error_messages:
            raise RuntimeError(
                "Import Pachete Saga finished with errors. "
                f"success={success_count}, failed={len(error_messages)}"
            )
        return success_count

    def run_sync_once(self, config: AppConfig, ignore_disabled: bool = False) -> None:
        self._ensure_dependencies()
        if not ignore_disabled and not config.enable_sync_job:
            self.log("Import Pachete Saga job is disabled. Skipping execution.")
            return

        pachet_url = config.pachet_import_api_url.strip()
        if not pachet_url:
            self.log("Import Pachete Saga API URL is empty. Set it in Import Pachete Saga tab.")
            return

        self.log("Import Pachete Saga job started.")
        pachete_processed = self._run_pachet_import_sync(config)
        self.log(f"Import Pachete Saga completed. Processed {pachete_processed} item(s).")
        self.log(
            "Import Pachete Saga summary: "
            f"processed={pachete_processed}."
        )

    def _query_stock(self, config: AppConfig) -> tuple[list[str], list[tuple[Any, ...]]]:
        sql = config.stock_select_sql.strip()
        if not sql:
            raise ValueError("Stock SELECT SQL is empty.")

        connection = self._connect(config)
        try:
            cursor = connection.cursor()
            cursor.execute(sql)
            rows = cursor.fetchall()
            description = cursor.description or []
        finally:
            connection.close()

        headers = [str(column[0]) for column in description]
        return headers, rows

    @staticmethod
    def _parse_extra_fields(raw: str) -> dict[str, str]:
        if not raw.strip():
            return {}
        parsed = json.loads(raw)
        if not isinstance(parsed, dict):
            raise ValueError("Extra upload fields must be a JSON object.")
        return {str(key): str(value) for key, value in parsed.items()}

    @staticmethod
    def _normalize_csv_value(value: Any) -> Any:
        """Remove right-side padding from text values before CSV export."""
        if isinstance(value, str):
            # Firebird CHAR columns are often right-padded with spaces.
            return value.rstrip(" \t")
        return value

    @staticmethod
    def _with_query_param(url: str, key: str, value: str) -> str:
        parts = urlsplit(url)
        query_items = parse_qsl(parts.query, keep_blank_values=True)
        filtered = [(k, v) for k, v in query_items if k != key]
        filtered.append((key, value))
        return urlunsplit(
            (
                parts.scheme,
                parts.netloc,
                parts.path,
                urlencode(filtered, doseq=True),
                parts.fragment,
            )
        )

    def _sanitize_url_for_log(self, url: str) -> str:
        parts = urlsplit(url)
        query_items = parse_qsl(parts.query, keep_blank_values=True)
        sanitized = []
        for key, value in query_items:
            if key.lower() in self._sensitive_query_keys:
                sanitized.append((key, "***"))
            else:
                sanitized.append((key, value))
        return urlunsplit(
            (
                parts.scheme,
                parts.netloc,
                parts.path,
                urlencode(sanitized, doseq=True),
                parts.fragment,
            )
        )

    @staticmethod
    def _truncate_for_log(text: str, max_length: int = 700) -> str:
        cleaned = text.strip()
        if not cleaned:
            return "<empty response body>"
        if len(cleaned) <= max_length:
            return cleaned
        return f"{cleaned[:max_length]}..."

    def _format_http_response_for_log(self, response: Any) -> str:
        content_type = str(response.headers.get("Content-Type", "unknown")).strip() or "unknown"
        try:
            payload = response.json()
            body_text = json.dumps(payload, ensure_ascii=True)
        except ValueError:
            body_text = response.text
        return (
            f"content-type={content_type}, "
            f"body={self._truncate_for_log(body_text)}"
        )

    @staticmethod
    def _extract_response_body(response: Any, max_length: int = 8000) -> str:
        try:
            payload = response.json()
            body_text = json.dumps(payload, ensure_ascii=True)
        except ValueError:
            body_text = response.text

        cleaned = body_text.strip()
        if not cleaned:
            return "<empty response body>"
        if len(cleaned) <= max_length:
            return cleaned
        return f"{cleaned[:max_length]}..."

    @staticmethod
    def _extract_response_metrics(response: Any) -> dict[str, Any]:
        try:
            payload = response.json()
        except ValueError:
            return {}
        if not isinstance(payload, dict):
            return {}

        metrics: dict[str, Any] = {}
        for key in (
            "products",
            "updated_lines",
            "inserted_lines",
            "created_lines",
            "errors",
            "ok",
            "status",
            "message",
        ):
            if key in payload:
                metrics[key] = payload[key]
        return metrics

    def _write_upload_audit_entry(
        self,
        config: AppConfig,
        *,
        csv_path: Path,
        row_count: int,
        upload_url: str,
        status: str,
        response: Any | None = None,
        error: str = "",
    ) -> Path:
        audit_dir = Path(config.audit_log_directory).expanduser().resolve()
        audit_dir.mkdir(parents=True, exist_ok=True)
        day_stamp = datetime.now().strftime("%Y%m%d")
        audit_path = audit_dir / f"upload_audit_{day_stamp}.jsonl"

        entry: dict[str, Any] = {
            "timestamp": datetime.now().isoformat(timespec="seconds"),
            "status": status,
            "upload_url": self._sanitize_url_for_log(upload_url),
            "csv_path": str(csv_path.resolve()),
            "csv_file_name": csv_path.name,
            "row_count": int(row_count),
        }
        if error:
            entry["error"] = error
        if response is not None:
            entry["http_status"] = int(response.status_code)
            entry["response_content_type"] = (
                str(response.headers.get("Content-Type", "unknown")).strip() or "unknown"
            )
            entry["response_body"] = self._extract_response_body(response)
            entry.update(self._extract_response_metrics(response))

        with audit_path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(entry, ensure_ascii=True))
            handle.write("\n")

        return audit_path

    def _record_upload_audit(
        self,
        config: AppConfig,
        *,
        csv_path: Path,
        row_count: int,
        upload_url: str,
        status: str,
        response: Any | None = None,
        error: str = "",
    ) -> None:
        try:
            audit_path = self._write_upload_audit_entry(
                config,
                csv_path=csv_path,
                row_count=row_count,
                upload_url=upload_url,
                status=status,
                response=response,
                error=error,
            )
            self.log(f"Upload audit saved: {audit_path}")
        except Exception as exc:  # pylint: disable=broad-except
            self.log(f"Warning: could not write upload audit entry: {exc}")

    def _write_csv(
        self,
        config: AppConfig,
        headers: list[str],
        rows: list[tuple[Any, ...]],
    ) -> Path:
        target_dir = Path(config.csv_directory).expanduser().resolve()
        target_dir.mkdir(parents=True, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        csv_path = target_dir / f"stock_export_{timestamp}.csv"

        with csv_path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.writer(handle)
            if headers:
                writer.writerow([self._normalize_csv_value(value) for value in headers])
            for row in rows:
                writer.writerow([self._normalize_csv_value(value) for value in row])

        self.log(f"CSV generated: {csv_path}")
        return csv_path

    def _upload_csv(self, config: AppConfig, csv_path: Path, row_count: int) -> None:
        url = config.upload_url.strip()
        if not url:
            raise ValueError("Upload URL is empty.")

        headers = {"Accept": "application/json"}
        user_agent = config.upload_user_agent.strip()
        if user_agent:
            headers["User-Agent"] = user_agent

        token = config.upload_api_token.strip() or config.sync_api_token.strip()
        token_query_param = config.upload_token_query_param.strip()
        final_url = url
        if token and token_query_param:
            final_url = self._with_query_param(url, token_query_param, token)
        elif token:
            headers["Authorization"] = f"Bearer {token}"
        headers.update(self._parse_extra_fields(config.upload_headers_json))

        form_data = self._parse_extra_fields(config.extra_upload_fields_json)

        response: Any | None = None
        try:
            with csv_path.open("rb") as handle:
                files = {
                    config.upload_field_name: (
                        csv_path.name,
                        handle,
                        "text/csv",
                    )
                }
                response = requests.post(
                    final_url,
                    headers=headers,
                    data=form_data if form_data else None,
                    files=files,
                    timeout=config.http_timeout_seconds,
                    verify=config.verify_ssl,
                )
                response.raise_for_status()
        except requests.HTTPError as exc:
            if response is None:
                raise RuntimeError(f"Upload failed: {exc}") from exc
            error_message = (
                f"Upload failed with HTTP {response.status_code}. "
                f"Server response: {self._format_http_response_for_log(response)}"
            )
            self._record_upload_audit(
                config,
                csv_path=csv_path,
                row_count=row_count,
                upload_url=final_url,
                status="http_error",
                response=response,
                error=error_message,
            )
            raise RuntimeError(error_message) from exc
        except requests.RequestException as exc:
            response = getattr(exc, "response", None)
            error_message = f"Upload request failed: {exc}"
            if response is not None:
                error_message = (
                    f"Upload failed with HTTP {response.status_code}. "
                    f"Server response: {self._format_http_response_for_log(response)}"
                )
            self._record_upload_audit(
                config,
                csv_path=csv_path,
                row_count=row_count,
                upload_url=final_url,
                status="request_error",
                response=response,
                error=error_message,
            )
            raise RuntimeError(error_message) from exc

        if response is None:
            raise RuntimeError("Upload failed: missing HTTP response.")

        self._record_upload_audit(
            config,
            csv_path=csv_path,
            row_count=row_count,
            upload_url=final_url,
            status="success",
            response=response,
        )
        sanitized_url = self._sanitize_url_for_log(final_url)
        self.log(
            f"CSV uploaded successfully to {sanitized_url} (status {response.status_code}, rows {row_count})."
        )
        self.log(f"Upload API response: {self._format_http_response_for_log(response)}")

    def run_export_once(self, config: AppConfig, ignore_disabled: bool = False) -> None:
        self._ensure_dependencies()
        if not ignore_disabled and not config.enable_export_job:
            self.log("Export job is disabled. Skipping execution.")
            return

        headers, rows = self._query_stock(config)
        csv_path = self._write_csv(config, headers, rows)
        self._upload_csv(config, csv_path, row_count=len(rows))
        self.log(f"Export job completed with {len(rows)} row(s).")


class SchedulerEngine:
    """Runs configured jobs periodically in background."""

    def __init__(self, service: IntegrationService, log_fn: Callable[[str], None]) -> None:
        self.service = service
        self.log = log_fn
        self._thread: threading.Thread | None = None
        self._stop_event = threading.Event()
        self._lock = threading.Lock()
        self._config = AppConfig()

    def is_running(self) -> bool:
        return self._thread is not None and self._thread.is_alive()

    def start(self, config: AppConfig) -> None:
        with self._lock:
            self.stop()
            self._config = config
            self._stop_event.clear()
            self._thread = threading.Thread(target=self._run_loop, daemon=True)
            self._thread.start()
        self.log("Scheduler started.")

    def stop(self) -> None:
        thread = self._thread
        if thread is None or not thread.is_alive():
            self._thread = None
            return
        self._stop_event.set()
        thread.join(timeout=5)
        self._thread = None
        self.log("Scheduler stopped.")

    def _run_job(self, name: str, callback: Callable[[AppConfig], None]) -> None:
        self.log(f"Running {name} job...")
        try:
            callback(self._config)
        except Exception as exc:  # pylint: disable=broad-except
            self.log(f"{name} job failed: {exc}")

    def _run_loop(self) -> None:
        next_sync = time.monotonic()
        next_export = time.monotonic()

        while not self._stop_event.is_set():
            now = time.monotonic()
            config = self._config

            if config.enable_sync_job and now >= next_sync:
                self._run_job("Import Pachete Saga", self.service.run_sync_once)
                next_sync = now + max(5, config.sync_interval_seconds)

            if config.enable_export_job and now >= next_export:
                self._run_job("export", self.service.run_export_once)
                next_export = now + max(10, config.export_interval_seconds)

            self._stop_event.wait(1)


class DesktopApp(tk.Tk):
    """Main Tkinter application."""

    def __init__(self, config_path: Path) -> None:
        super().__init__()
        self.title("Desktop Stock ERP Integration")
        self.geometry("980x840")
        self.minsize(900, 720)

        self.config_path = config_path
        self.config_data = load_config(config_path)
        self.log_queue: queue.Queue[str] = queue.Queue()

        self.service = IntegrationService(self._enqueue_log)
        self.scheduler = SchedulerEngine(self.service, self._enqueue_log)

        self._build_variables()
        self._build_ui()
        self._load_form(self.config_data)
        self._set_button_states()
        self._enqueue_log("Application initialized.")

        self.after(200, self._flush_log_queue)
        self.protocol("WM_DELETE_WINDOW", self._on_close)

    def _build_variables(self) -> None:
        self.var_db_path = tk.StringVar()
        self.var_db_host = tk.StringVar()
        self.var_db_port = tk.StringVar()
        self.var_db_user = tk.StringVar()
        self.var_db_password = tk.StringVar()
        self.var_db_charset = tk.StringVar()
        self.var_fb_client_library = tk.StringVar()

        self.var_sync_enabled = tk.BooleanVar()
        self.var_pachet_import_api_url = tk.StringVar()
        self.var_sync_api_token = tk.StringVar()
        self.var_sync_interval = tk.StringVar()

        self.var_export_enabled = tk.BooleanVar()
        self.var_export_interval = tk.StringVar()
        self.var_upload_url = tk.StringVar()
        self.var_upload_field_name = tk.StringVar()
        self.var_upload_api_token = tk.StringVar()
        self.var_upload_token_query_param = tk.StringVar()
        self.var_upload_headers_json = tk.StringVar()
        self.var_upload_user_agent = tk.StringVar()
        self.var_csv_directory = tk.StringVar()
        self.var_audit_log_directory = tk.StringVar()
        self.var_http_timeout = tk.StringVar()
        self.var_verify_ssl = tk.BooleanVar()
        self.var_extra_upload_fields = tk.StringVar()

    def _build_ui(self) -> None:
        container = ttk.Frame(self, padding=12)
        container.pack(fill="both", expand=True)
        container.columnconfigure(0, weight=1)

        button_row = ttk.Frame(container)
        button_row.grid(row=0, column=0, sticky="ew", pady=(0, 10))

        self.btn_save = ttk.Button(button_row, text="Save config", command=self._on_save)
        self.btn_save.pack(side="left", padx=(0, 8))

        self.btn_start = ttk.Button(button_row, text="Start scheduler", command=self._on_start)
        self.btn_start.pack(side="left", padx=(0, 8))

        self.btn_stop = ttk.Button(button_row, text="Stop scheduler", command=self._on_stop)
        self.btn_stop.pack(side="left", padx=(0, 8))

        self.btn_run_sync = ttk.Button(
            button_row,
            text="Run import now",
            command=self._run_sync_now,
        )
        self.btn_run_sync.pack(side="left", padx=(0, 8))

        self.btn_run_export = ttk.Button(
            button_row,
            text="Run export now",
            command=self._run_export_now,
        )
        self.btn_run_export.pack(side="left")

        notebook = ttk.Notebook(container)
        notebook.grid(row=1, column=0, sticky="nsew")
        container.rowconfigure(1, weight=1)

        firebird_frame = ttk.Frame(notebook, padding=12)
        sync_frame = ttk.Frame(notebook, padding=12)
        export_frame = ttk.Frame(notebook, padding=12)
        logs_frame = ttk.Frame(notebook, padding=12)

        notebook.add(firebird_frame, text="Firebird")
        notebook.add(sync_frame, text="Import Pachete Saga")
        notebook.add(export_frame, text="Stock export")
        notebook.add(logs_frame, text="Logs")

        self._build_firebird_tab(firebird_frame)
        self._build_sync_tab(sync_frame)
        self._build_export_tab(export_frame)
        self._build_logs_tab(logs_frame)

    @staticmethod
    def _add_entry_row(
        parent: ttk.Frame,
        row: int,
        label: str,
        variable: tk.StringVar,
        show: str | None = None,
    ) -> ttk.Entry:
        ttk.Label(parent, text=label).grid(row=row, column=0, sticky="w", pady=4)
        entry = ttk.Entry(parent, textvariable=variable, width=78, show=show)
        entry.grid(row=row, column=1, sticky="ew", pady=4)
        return entry

    def _build_firebird_tab(self, frame: ttk.Frame) -> None:
        frame.columnconfigure(1, weight=1)

        ttk.Label(frame, text="Database file (.fdb)").grid(row=0, column=0, sticky="w", pady=4)
        ttk.Entry(frame, textvariable=self.var_db_path, width=78).grid(
            row=0,
            column=1,
            sticky="ew",
            pady=4,
        )
        ttk.Button(frame, text="Browse...", command=self._browse_db_file).grid(
            row=0,
            column=2,
            sticky="ew",
            padx=(6, 0),
            pady=4,
        )

        self._add_entry_row(frame, 1, "Host", self.var_db_host)
        self._add_entry_row(frame, 2, "Port", self.var_db_port)
        self._add_entry_row(frame, 3, "User", self.var_db_user)
        self._add_entry_row(frame, 4, "Password", self.var_db_password, show="*")
        self._add_entry_row(frame, 5, "Charset", self.var_db_charset)

        ttk.Label(frame, text="Firebird client library (fbclient.dll)").grid(
            row=6,
            column=0,
            sticky="w",
            pady=4,
        )
        ttk.Entry(frame, textvariable=self.var_fb_client_library, width=78).grid(
            row=6,
            column=1,
            sticky="ew",
            pady=4,
        )
        ttk.Button(frame, text="Browse...", command=self._browse_fb_client_library).grid(
            row=6,
            column=2,
            sticky="ew",
            padx=(6, 0),
            pady=4,
        )

        note = (
            "Tip: leave Host empty for direct file connection (embedded). "
            "Set Host + Port only when you want server mode. "
            "If you get client library errors, select fbclient.dll."
        )
        ttk.Label(frame, text=note, wraplength=780, foreground="#4B5563").grid(
            row=7,
            column=0,
            columnspan=3,
            sticky="w",
            pady=(10, 0),
        )

    def _build_sync_tab(self, frame: ttk.Frame) -> None:
        frame.columnconfigure(1, weight=1)

        ttk.Checkbutton(frame, text="Enable Import Pachete Saga job", variable=self.var_sync_enabled).grid(
            row=0,
            column=0,
            columnspan=2,
            sticky="w",
            pady=(0, 10),
        )
        self._add_entry_row(frame, 1, "Import API URL", self.var_pachet_import_api_url)
        self._add_entry_row(frame, 2, "Import API token (optional)", self.var_sync_api_token)
        self._add_entry_row(frame, 3, "Import interval (seconds)", self.var_sync_interval)

        payload_help = (
            "Expected payload:\n"
            "{\"pachet\": {..., \"status\":\"pending\"}, \"produse\": [...]} "
            "or {\"pachete\": [ ... ]}."
        )
        ttk.Label(frame, text=payload_help, wraplength=780, foreground="#4B5563").grid(
            row=4,
            column=0,
            columnspan=2,
            sticky="w",
            pady=(10, 0),
        )

    def _build_export_tab(self, frame: ttk.Frame) -> None:
        frame.columnconfigure(1, weight=1)

        ttk.Checkbutton(frame, text="Enable periodic stock export job", variable=self.var_export_enabled).grid(
            row=0,
            column=0,
            columnspan=2,
            sticky="w",
            pady=(0, 10),
        )
        self._add_entry_row(frame, 1, "Export interval (seconds)", self.var_export_interval)
        self._add_entry_row(frame, 2, "Upload URL (PHP endpoint)", self.var_upload_url)
        self._add_entry_row(frame, 3, "Upload file field name", self.var_upload_field_name)
        self._add_entry_row(frame, 4, "Upload API token (optional)", self.var_upload_api_token)
        self._add_entry_row(
            frame,
            5,
            "Upload token query param (optional, ex: token)",
            self.var_upload_token_query_param,
        )
        self._add_entry_row(
            frame,
            6,
            "Upload headers JSON (optional)",
            self.var_upload_headers_json,
        )
        self._add_entry_row(frame, 7, "Upload User-Agent", self.var_upload_user_agent)

        ttk.Label(frame, text="CSV directory").grid(row=8, column=0, sticky="w", pady=4)
        ttk.Entry(frame, textvariable=self.var_csv_directory, width=78).grid(
            row=8,
            column=1,
            sticky="ew",
            pady=4,
        )
        ttk.Button(frame, text="Browse...", command=self._browse_csv_folder).grid(
            row=8,
            column=2,
            sticky="ew",
            padx=(6, 0),
            pady=4,
        )

        self._add_entry_row(frame, 9, "Audit log directory", self.var_audit_log_directory)
        self._add_entry_row(frame, 10, "HTTP timeout (seconds)", self.var_http_timeout)
        ttk.Checkbutton(frame, text="Verify SSL certificate", variable=self.var_verify_ssl).grid(
            row=11,
            column=0,
            columnspan=2,
            sticky="w",
            pady=4,
        )
        self._add_entry_row(
            frame,
            12,
            "Extra upload fields JSON (optional)",
            self.var_extra_upload_fields,
        )

        ttk.Label(frame, text="Stock SELECT SQL").grid(row=13, column=0, sticky="nw", pady=(10, 4))
        self.txt_stock_sql = tk.Text(frame, height=10, wrap="word")
        self.txt_stock_sql.grid(row=13, column=1, sticky="nsew", pady=(10, 4))
        frame.rowconfigure(13, weight=1)

    def _build_logs_tab(self, frame: ttk.Frame) -> None:
        frame.columnconfigure(0, weight=1)
        frame.rowconfigure(0, weight=1)
        self.txt_logs = tk.Text(frame, wrap="word", state="disabled")
        self.txt_logs.grid(row=0, column=0, sticky="nsew")

    def _browse_db_file(self) -> None:
        selected = filedialog.askopenfilename(
            title="Select Firebird database file",
            filetypes=[("Firebird Database", "*.fdb"), ("All files", "*.*")],
        )
        if selected:
            self.var_db_path.set(selected)

    def _browse_fb_client_library(self) -> None:
        selected = filedialog.askopenfilename(
            title="Select Firebird client library",
            filetypes=[("Firebird client library", "fbclient.dll"), ("DLL files", "*.dll"), ("All files", "*.*")],
        )
        if selected:
            self.var_fb_client_library.set(selected)

    def _browse_csv_folder(self) -> None:
        selected = filedialog.askdirectory(title="Select CSV output folder")
        if selected:
            self.var_csv_directory.set(selected)

    def _load_form(self, config: AppConfig) -> None:
        self.var_db_path.set(config.db_path)
        self.var_db_host.set(config.db_host)
        self.var_db_port.set(str(config.db_port))
        self.var_db_user.set(config.db_user)
        self.var_db_password.set(config.db_password)
        self.var_db_charset.set(config.db_charset)
        self.var_fb_client_library.set(config.fb_client_library_path)

        self.var_sync_enabled.set(config.enable_sync_job)
        self.var_pachet_import_api_url.set(config.pachet_import_api_url)
        self.var_sync_api_token.set(config.sync_api_token)
        self.var_sync_interval.set(str(config.sync_interval_seconds))

        self.var_export_enabled.set(config.enable_export_job)
        self.var_export_interval.set(str(config.export_interval_seconds))
        self.var_upload_url.set(config.upload_url)
        self.var_upload_field_name.set(config.upload_field_name)
        self.var_upload_api_token.set(config.upload_api_token)
        self.var_upload_token_query_param.set(config.upload_token_query_param)
        self.var_upload_headers_json.set(config.upload_headers_json)
        self.var_upload_user_agent.set(config.upload_user_agent)
        self.var_csv_directory.set(config.csv_directory)
        self.var_audit_log_directory.set(config.audit_log_directory)
        self.var_http_timeout.set(str(config.http_timeout_seconds))
        self.var_verify_ssl.set(config.verify_ssl)
        self.var_extra_upload_fields.set(config.extra_upload_fields_json)

        self.txt_stock_sql.delete("1.0", "end")
        self.txt_stock_sql.insert("1.0", config.stock_select_sql)

    def _collect_form(self) -> AppConfig:
        db_port = to_int(self.var_db_port.get(), default=3050, minimum=1)
        sync_interval = to_int(self.var_sync_interval.get(), default=120, minimum=5)
        export_interval = to_int(self.var_export_interval.get(), default=300, minimum=10)
        timeout = to_int(self.var_http_timeout.get(), default=30, minimum=5)

        stock_sql = self.txt_stock_sql.get("1.0", "end").strip()
        upload_field = self.var_upload_field_name.get().strip() or "file"

        return AppConfig(
            db_path=self.var_db_path.get().strip(),
            db_host=self.var_db_host.get().strip(),
            db_port=db_port,
            db_user=self.var_db_user.get().strip() or "SYSDBA",
            db_password=self.var_db_password.get(),
            db_charset=self.var_db_charset.get().strip() or "UTF8",
            fb_client_library_path=self.var_fb_client_library.get().strip(),
            enable_sync_job=self.var_sync_enabled.get(),
            sync_interval_seconds=sync_interval,
            pachet_import_api_url=self.var_pachet_import_api_url.get().strip(),
            sync_api_token=self.var_sync_api_token.get().strip(),
            enable_export_job=self.var_export_enabled.get(),
            export_interval_seconds=export_interval,
            stock_select_sql=stock_sql,
            upload_url=self.var_upload_url.get().strip(),
            upload_field_name=upload_field,
            upload_api_token=self.var_upload_api_token.get().strip(),
            upload_token_query_param=self.var_upload_token_query_param.get().strip(),
            upload_headers_json=self.var_upload_headers_json.get().strip(),
            upload_user_agent=self.var_upload_user_agent.get().strip()
            or "Mozilla/5.0 (Windows NT 10.0; Win64; x64) DesktopStockErpIntegration/1.0",
            csv_directory=self.var_csv_directory.get().strip() or "exports",
            audit_log_directory=self.var_audit_log_directory.get().strip() or "audit_logs",
            http_timeout_seconds=timeout,
            verify_ssl=self.var_verify_ssl.get(),
            extra_upload_fields_json=self.var_extra_upload_fields.get().strip(),
        )

    def _save_config(self, config: AppConfig) -> None:
        save_config(self.config_path, config)
        self.config_data = config
        self._enqueue_log(f"Configuration saved to {self.config_path.resolve()}.")

    def _set_button_states(self) -> None:
        running = self.scheduler.is_running()
        if running:
            self.btn_start.configure(state="disabled")
            self.btn_stop.configure(state="normal")
        else:
            self.btn_start.configure(state="normal")
            self.btn_stop.configure(state="disabled")

    def _validate_config(self, config: AppConfig) -> None:
        if not config.db_path:
            raise ValueError("Please select a Firebird database file.")
        if config.enable_sync_job and not config.pachet_import_api_url.strip():
            raise ValueError("Import API URL is required when Import Pachete Saga job is enabled.")
        if config.enable_export_job and not config.stock_select_sql:
            raise ValueError("Stock SELECT SQL is required when export job is enabled.")
        if config.enable_export_job and not config.upload_url:
            raise ValueError("Upload URL is required when export job is enabled.")

    def _on_save(self) -> None:
        try:
            config = self._collect_form()
            self._validate_config(config)
            self._save_config(config)
            if self.scheduler.is_running():
                self.scheduler.start(config)
                self._enqueue_log("Scheduler restarted with updated config.")
        except Exception as exc:  # pylint: disable=broad-except
            messagebox.showerror("Save failed", str(exc))

    def _on_start(self) -> None:
        try:
            config = self._collect_form()
            self._validate_config(config)
            self._save_config(config)
            self.scheduler.start(config)
            self._set_button_states()
        except Exception as exc:  # pylint: disable=broad-except
            messagebox.showerror("Cannot start scheduler", str(exc))

    def _on_stop(self) -> None:
        self.scheduler.stop()
        self._set_button_states()

    def _run_sync_now(self) -> None:
        self._run_manual_job("import_pachete")

    def _run_export_now(self) -> None:
        self._run_manual_job("export")

    def _run_manual_job(self, name: str) -> None:
        try:
            config = self._collect_form()
            self._validate_config(config)
            self._save_config(config)
        except Exception as exc:  # pylint: disable=broad-except
            messagebox.showerror("Cannot run job", str(exc))
            return

        job_display_name = {
            "import_pachete": "Import Pachete Saga",
            "export": "Export stocuri",
        }.get(name, name)

        def _runner() -> None:
            self._enqueue_log(f"Manual {job_display_name} job started.")
            try:
                if name == "import_pachete":
                    self.service.run_sync_once(config, ignore_disabled=True)
                else:
                    self.service.run_export_once(config, ignore_disabled=True)
                self._enqueue_log(f"Manual {job_display_name} job finished.")
            except Exception as inner_exc:  # pylint: disable=broad-except
                self._enqueue_log(f"Manual {job_display_name} job failed: {inner_exc}")

        thread = threading.Thread(target=_runner, daemon=True)
        thread.start()

    def _enqueue_log(self, message: str) -> None:
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.log_queue.put(f"{timestamp} | {message}")

    def _flush_log_queue(self) -> None:
        has_messages = False
        while True:
            try:
                message = self.log_queue.get_nowait()
            except queue.Empty:
                break
            has_messages = True
            self.txt_logs.configure(state="normal")
            self.txt_logs.insert("end", f"{message}\n")
            self.txt_logs.configure(state="disabled")
            self.txt_logs.see("end")
        if has_messages:
            self._set_button_states()
        self.after(200, self._flush_log_queue)

    def _on_close(self) -> None:
        self.scheduler.stop()
        self.destroy()


def main() -> None:
    app = DesktopApp(CONFIG_PATH)
    app.mainloop()


if __name__ == "__main__":
    main()
