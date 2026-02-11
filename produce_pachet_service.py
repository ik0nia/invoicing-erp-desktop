#!/usr/bin/env python3
"""Transactional Firebird implementation for producePachet."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from pathlib import Path
from typing import Any

try:
    from firebird.driver import connect as fb_connect
    from firebird.driver import driver_config as fb_driver_config
except Exception as exc:  # pragma: no cover - import guard
    fb_connect = None
    fb_driver_config = None
    FIREBIRD_IMPORT_ERROR = exc
else:
    FIREBIRD_IMPORT_ERROR = None


SQL_QUERIES: dict[str, str] = {
    "select_pachet_by_denumire": """
SELECT FIRST 1 COD, UM
FROM ARTICOLE
WHERE TRIM(DENUMIRE) = TRIM(?)
ORDER BY COD
""".strip(),
    "select_max_articole_cod8": """
SELECT COALESCE(MAX(CAST(SUBSTRING(TRIM(COD) FROM 1 FOR 8) AS INTEGER)), 0)
FROM ARTICOLE
WHERE CHAR_LENGTH(TRIM(COD)) >= 8
  AND SUBSTRING(TRIM(COD) FROM 1 FOR 8) BETWEEN '00000000' AND '99999999'
""".strip(),
    "insert_articol_pachet": """
INSERT INTO ARTICOLE (
    COD,
    DENUMIRE,
    UM,
    TVA,
    DEN_TIP,
    TIP,
    PRET_VANZ,
    PRET_V_TVA
) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
""".strip(),
    "select_max_nr_doc_bp_by_date": """
SELECT COALESCE(MAX(NR_DOC), 0)
FROM MISCARI
WHERE DATA = ?
  AND TIP_DOC = ?
""".strip(),
    "check_miscari_has_pret_column": """
SELECT COUNT(*)
FROM RDB$RELATION_FIELDS
WHERE TRIM(RDB$RELATION_NAME) = ?
  AND TRIM(RDB$FIELD_NAME) = ?
""".strip(),
    "insert_miscari_consum_bc": """
INSERT INTO MISCARI (
    ID,
    DATA,
    NR_DOC,
    TIP_DOC,
    COD_ART,
    CANTITATE,
    GESTIUNE
) VALUES (?, ?, ?, ?, ?, ?, ?)
""".strip(),
    "insert_miscari_produs_bp_with_pret": """
INSERT INTO MISCARI (
    ID,
    DATA,
    NR_DOC,
    TIP_DOC,
    COD_ART,
    CANTITATE,
    GESTIUNE,
    PRET
) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
""".strip(),
    "insert_miscari_produs_bp_without_pret": """
INSERT INTO MISCARI (
    ID,
    DATA,
    NR_DOC,
    TIP_DOC,
    COD_ART,
    CANTITATE,
    GESTIUNE
) VALUES (?, ?, ?, ?, ?, ?, ?)
""".strip(),
}

_MONEY_Q = Decimal("0.0001")
_VALID_TVA = {Decimal("0"), Decimal("11"), Decimal("21")}


@dataclass(frozen=True)
class FirebirdConnectionSettings:
    """Connection settings used by producePachet."""

    database_path: str
    host: str = ""
    port: int = 3050
    user: str = "SYSDBA"
    password: str = "masterkey"
    charset: str = "UTF8"
    fb_client_library_path: str = ""


@dataclass(frozen=True)
class PachetInput:
    id_doc: int
    data: date
    denumire: str
    pret_vanz: Decimal
    cota_tva: Decimal
    cost_total: Decimal
    gestiune: str
    cantitate_produsa: Decimal
    status: str


@dataclass(frozen=True)
class ProdusInput:
    cod_articol_raw: str
    cod_articol_db: str
    cantitate: Decimal
    val_produse: Decimal


@dataclass(frozen=True)
class ProducePachetInput:
    pachet: PachetInput
    produse: list[ProdusInput]


def get_produce_pachet_sql_queries() -> dict[str, str]:
    """Return all SQL strings used by producePachet."""
    return dict(SQL_QUERIES)


def _ensure_firebird_available() -> None:
    if fb_connect is None:
        raise RuntimeError(f"Missing dependency 'firebird-driver': {FIREBIRD_IMPORT_ERROR}")


def _is_unique_violation(exc: Exception) -> bool:
    message = str(exc).lower()
    return (
        "violation of primary or unique key constraint" in message
        or "unique key" in message
        or "duplicate" in message
        or "sqlstate = 23000" in message
    )


def _quantize_money(value: Decimal) -> Decimal:
    return value.quantize(_MONEY_Q, rounding=ROUND_HALF_UP)


def _to_decimal(value: Any, field_name: str) -> Decimal:
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError) as exc:
        raise ValueError(f"Field '{field_name}' must be numeric.") from exc


def _parse_date(value: Any, field_name: str) -> date:
    if not isinstance(value, str):
        raise ValueError(f"Field '{field_name}' must be a string in format YYYY-MM-DD.")
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError as exc:
        raise ValueError(f"Field '{field_name}' has invalid date format. Use YYYY-MM-DD.") from exc


def _parse_int(value: Any, field_name: str) -> int:
    try:
        parsed = int(value)
    except (ValueError, TypeError) as exc:
        raise ValueError(f"Field '{field_name}' must be integer.") from exc
    return parsed


def _normalize_fixed_char(value: str, max_length: int) -> str:
    trimmed = value.strip()
    if not trimmed:
        raise ValueError("String field cannot be empty.")
    if len(trimmed) > max_length:
        raise ValueError(f"Value '{trimmed}' exceeds max length {max_length}.")
    return trimmed


def normalizeCodArticol(cod: Any) -> str:
    """
    Normalize COD to DB fixed format (CHAR(16)).

    - preferred source: 8 digits
    - output format: 8 digits + spaces to 16 chars
    - if input is already 16 chars, keeps it (after minimal sanity checks)
    """

    if cod is None:
        raise ValueError("cod_articol is required.")

    raw = str(cod)
    if len(raw) == 16:
        first8 = raw[:8]
        if not first8.isdigit():
            raise ValueError(f"Invalid cod_articol '{raw}'. First 8 chars must be digits.")
        return raw

    trimmed = raw.strip()
    if not trimmed:
        raise ValueError("cod_articol cannot be empty.")
    if not trimmed.isdigit():
        raise ValueError(f"Invalid cod_articol '{trimmed}'. Only digits are allowed.")
    if len(trimmed) > 8:
        raise ValueError(f"Invalid cod_articol '{trimmed}'. Expected max 8 digits or fixed CHAR(16).")

    code8 = trimmed.zfill(8)
    return code8.ljust(16)


def _build_database_target(settings: FirebirdConnectionSettings) -> str:
    db_path = settings.database_path.strip()
    if not db_path:
        raise ValueError("database_path is required.")

    host = settings.host.strip()
    if host:
        return f"{host}/{settings.port}:{db_path}"
    return db_path


def _configure_firebird_client_library(settings: FirebirdConnectionSettings) -> None:
    path_raw = settings.fb_client_library_path.strip()
    if not path_raw:
        return

    path = Path(path_raw).expanduser()
    if not path.exists():
        raise ValueError(f"Firebird client library not found at: {path}")
    if fb_driver_config is not None:
        fb_driver_config.fb_client_library.value = str(path)


def validate_produce_pachet_input(payload: Any) -> ProducePachetInput:
    """Validate the input JSON payload and return normalized dataclasses."""
    if not isinstance(payload, dict):
        raise ValueError("Payload must be a JSON object.")

    pachet_raw = payload.get("pachet")
    produse_raw = payload.get("produse")

    if not isinstance(pachet_raw, dict):
        raise ValueError("Field 'pachet' is required and must be an object.")
    if not isinstance(produse_raw, list) or not produse_raw:
        raise ValueError("Field 'produse' is required and must be a non-empty array.")

    pachet = PachetInput(
        id_doc=_parse_int(pachet_raw.get("id_doc"), "pachet.id_doc"),
        data=_parse_date(pachet_raw.get("data"), "pachet.data"),
        denumire=_normalize_fixed_char(str(pachet_raw.get("denumire", "")), 255),
        pret_vanz=_to_decimal(pachet_raw.get("pret_vanz"), "pachet.pret_vanz"),
        cota_tva=_to_decimal(pachet_raw.get("cota_tva"), "pachet.cota_tva"),
        cost_total=_to_decimal(pachet_raw.get("cost_total"), "pachet.cost_total"),
        gestiune=_normalize_fixed_char(str(pachet_raw.get("gestiune", "")), 16),
        cantitate_produsa=_to_decimal(
            pachet_raw.get("cantitate_produsa"),
            "pachet.cantitate_produsa",
        ),
        status=str(pachet_raw.get("status", "")).strip().lower(),
    )

    if pachet.id_doc <= 0:
        raise ValueError("pachet.id_doc must be > 0.")
    if pachet.status not in {"pending", "processing"}:
        raise ValueError("pachet.status must be 'processing' or 'pending'.")
    if pachet.cantitate_produsa <= 0:
        raise ValueError("pachet.cantitate_produsa must be > 0.")
    if pachet.pret_vanz < 0:
        raise ValueError("pachet.pret_vanz must be >= 0.")
    if pachet.cost_total < 0:
        raise ValueError("pachet.cost_total must be >= 0.")
    if _quantize_money(pachet.cota_tva) not in _VALID_TVA:
        raise ValueError("pachet.cota_tva must be one of: 0, 11, 21.")

    produse: list[ProdusInput] = []
    total_val_produse = Decimal("0")
    for index, produs_raw in enumerate(produse_raw, start=1):
        if not isinstance(produs_raw, dict):
            raise ValueError(f"produse[{index}] must be an object.")

        cantitate = _to_decimal(produs_raw.get("cantitate"), f"produse[{index}].cantitate")
        if cantitate <= 0:
            raise ValueError(f"produse[{index}].cantitate must be > 0.")

        val_produse = _to_decimal(produs_raw.get("val_produse"), f"produse[{index}].val_produse")
        if val_produse < 0:
            raise ValueError(f"produse[{index}].val_produse must be >= 0.")

        cod_raw = str(produs_raw.get("cod_articol", ""))
        cod_db = normalizeCodArticol(cod_raw)

        produs = ProdusInput(
            cod_articol_raw=cod_raw.strip(),
            cod_articol_db=cod_db,
            cantitate=cantitate,
            val_produse=val_produse,
        )
        produse.append(produs)
        total_val_produse += val_produse

    if _quantize_money(total_val_produse) != _quantize_money(pachet.cost_total):
        raise ValueError(
            "pachet.cost_total does not match SUM(produse[*].val_produse). "
            f"Expected {_quantize_money(total_val_produse)}, got {_quantize_money(pachet.cost_total)}."
        )

    return ProducePachetInput(pachet=pachet, produse=produse)


def ensurePachetInArticole(cursor: Any, pachet: PachetInput) -> str:
    """
    Ensure package exists in ARTICOLE and return COD in DB CHAR(16) format.

    Concurrency handling:
    - if INSERT fails with duplicate key, re-read by DENUMIRE and continue.
    """

    cursor.execute(SQL_QUERIES["select_pachet_by_denumire"], [pachet.denumire])
    row = cursor.fetchone()
    if row:
        cod = str(row[0] or "")
        if len(cod) < 16:
            cod = cod.strip().ljust(16)
        return cod[:16]

    cursor.execute(SQL_QUERIES["select_max_articole_cod8"])
    max_code = int(cursor.fetchone()[0] or 0)
    code8 = f"{max_code + 1:08d}"
    code_db = code8.ljust(16)
    um = "BUC"

    try:
        cursor.execute(
            SQL_QUERIES["insert_articol_pachet"],
            [
                code_db,
                pachet.denumire,
                um,
                pachet.cota_tva,
                "Produse finite",
                "04",
                pachet.pret_vanz,
                pachet.pret_vanz,
            ],
        )
        return code_db
    except Exception as exc:
        if not _is_unique_violation(exc):
            raise

        # Another concurrent transaction inserted between MAX+1 and INSERT.
        cursor.execute(SQL_QUERIES["select_pachet_by_denumire"], [pachet.denumire])
        retry_row = cursor.fetchone()
        if retry_row:
            cod = str(retry_row[0] or "")
            if len(cod) < 16:
                cod = cod.strip().ljust(16)
            return cod[:16]
        raise


def getNextNrDoc(cursor: Any, data_doc: date) -> int:
    """Return next NR_DOC for TIP_DOC='BP' on requested date."""
    cursor.execute(SQL_QUERIES["select_max_nr_doc_bp_by_date"], [data_doc, "BP"])
    max_nr = int(cursor.fetchone()[0] or 0)
    return max_nr + 1


def _miscari_has_pret_column(cursor: Any) -> bool:
    cursor.execute(SQL_QUERIES["check_miscari_has_pret_column"], ["MISCARI", "PRET"])
    return int(cursor.fetchone()[0] or 0) > 0


def _trim_db_char(value: str) -> str:
    return str(value or "").rstrip()


def _execute_produce_pachet_once(cursor: Any, request: ProducePachetInput) -> dict[str, Any]:
    pachet = request.pachet
    cod_pachet_db = ensurePachetInArticole(cursor, pachet)
    nr_doc = getNextNrDoc(cursor, pachet.data)
    miscari_has_pret = _miscari_has_pret_column(cursor)

    for produs in request.produse:
        qty_consum = -abs(produs.cantitate)
        cursor.execute(
            SQL_QUERIES["insert_miscari_consum_bc"],
            [
                pachet.id_doc,
                pachet.data,
                nr_doc,
                "BC",
                produs.cod_articol_db,
                qty_consum,
                pachet.gestiune,
            ],
        )

    qty_produs = abs(pachet.cantitate_produsa)
    if miscari_has_pret:
        cursor.execute(
            SQL_QUERIES["insert_miscari_produs_bp_with_pret"],
            [
                pachet.id_doc,
                pachet.data,
                nr_doc,
                "BP",
                cod_pachet_db,
                qty_produs,
                pachet.gestiune,
                pachet.pret_vanz,
            ],
        )
    else:
        cursor.execute(
            SQL_QUERIES["insert_miscari_produs_bp_without_pret"],
            [
                pachet.id_doc,
                pachet.data,
                nr_doc,
                "BP",
                cod_pachet_db,
                qty_produs,
                pachet.gestiune,
            ],
        )

    return {
        "success": True,
        "message": "producePachet executed successfully.",
        "codPachet": _trim_db_char(cod_pachet_db),
        "nrDoc": nr_doc,
        "idDoc": pachet.id_doc,
    }


def producePachet(payload: Any, db_settings: FirebirdConnectionSettings) -> dict[str, Any]:
    """
    Create production package movements in one Firebird transaction.

    Transaction behavior:
    - BEGIN transaction (default driver tx)
    - on any failure => ROLLBACK
    - otherwise => COMMIT
    """

    _ensure_firebird_available()
    _configure_firebird_client_library(db_settings)
    request = validate_produce_pachet_input(payload)
    db_target = _build_database_target(db_settings)

    last_error: Exception | None = None
    for attempt in range(2):
        connection = fb_connect(
            database=db_target,
            user=db_settings.user,
            password=db_settings.password,
            charset=db_settings.charset,
        )
        try:
            cursor = connection.cursor()
            result = _execute_produce_pachet_once(cursor, request)
            connection.commit()
            return result
        except Exception as exc:
            connection.rollback()
            last_error = exc
            # Rare case: NR_DOC collision under concurrency. Retry once.
            if _is_unique_violation(exc) and attempt == 0:
                continue
            raise
        finally:
            connection.close()

    if last_error is not None:
        raise last_error
    raise RuntimeError("producePachet failed with unknown error.")

