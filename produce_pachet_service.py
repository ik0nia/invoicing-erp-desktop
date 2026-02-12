#!/usr/bin/env python3
"""Transactional Firebird implementation for producePachet."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time as dt_time
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
    TIP
) VALUES (?, ?, ?, ?, ?, ?)
""".strip(),
    "select_max_nr_doc_bp_by_date": """
SELECT COALESCE(MAX(NR_DOC), 0)
FROM MISCARI
WHERE DATA = ?
  AND TIP_DOC = ?
""".strip(),
    "select_existing_doc_summary": """
SELECT
    COALESCE(MIN(NR_DOC), 0),
    COALESCE(SUM(CASE WHEN TIP_DOC = 'BC' THEN 1 ELSE 0 END), 0),
    COALESCE(SUM(CASE WHEN TIP_DOC = 'BP' THEN 1 ELSE 0 END), 0)
FROM MISCARI
WHERE ID = ?
  AND DATA = ?
  AND TIP_DOC IN ('BC', 'BP')
""".strip(),
    "select_existing_bp_doc": """
SELECT FIRST 1 ID, NR_DOC, COD_ART
FROM MISCARI
WHERE ID = ?
  AND DATA = ?
  AND TIP_DOC = 'BP'
ORDER BY NR_DOC DESC
""".strip(),
    "select_pred_det_existing_nr_doc_by_id_unic": """
SELECT FIRST 1 NR_DOC
FROM PRED_DET
WHERE ID_UNIC = ?
ORDER BY NR_DOC DESC
""".strip(),
    "select_pred_det_existing_nr_doc_by_id_doc": """
SELECT FIRST 1 NR_DOC
FROM PRED_DET
WHERE ID_DOC = ?
ORDER BY NR_DOC DESC
""".strip(),
    "select_doc_counts_by_date_nr_doc": """
SELECT
    COALESCE(SUM(CASE WHEN TIP_DOC = 'BC' THEN 1 ELSE 0 END), 0),
    COALESCE(SUM(CASE WHEN TIP_DOC = 'BP' THEN 1 ELSE 0 END), 0)
FROM MISCARI
WHERE DATA = ?
  AND NR_DOC = ?
  AND TIP_DOC IN ('BC', 'BP')
""".strip(),
    "select_bp_doc_by_date_nr_doc": """
SELECT FIRST 1 ID, COD_ART
FROM MISCARI
WHERE DATA = ?
  AND NR_DOC = ?
  AND TIP_DOC = 'BP'
ORDER BY ID DESC
""".strip(),
    "check_miscari_has_pret_column": """
SELECT COUNT(*)
FROM RDB$RELATION_FIELDS
WHERE TRIM(RDB$RELATION_NAME) = ?
  AND TRIM(RDB$FIELD_NAME) = ?
""".strip(),
    "select_max_miscari_id_u": """
SELECT COALESCE(MAX(ID_U), 0)
FROM MISCARI
""".strip(),
    "select_max_miscari_id": """
SELECT COALESCE(MAX(ID), 0)
FROM MISCARI
""".strip(),
    "select_relation_fields": """
SELECT
    TRIM(rf.RDB$FIELD_NAME) AS FIELD_NAME,
    COALESCE(rf.RDB$NULL_FLAG, 0) AS NULL_FLAG,
    rf.RDB$DEFAULT_SOURCE AS DEFAULT_SOURCE,
    COALESCE(rf.RDB$IDENTITY_TYPE, -1) AS IDENTITY_TYPE,
    COALESCE(f.RDB$FIELD_TYPE, 0) AS FIELD_TYPE
FROM RDB$RELATION_FIELDS rf
JOIN RDB$FIELDS f ON f.RDB$FIELD_NAME = rf.RDB$FIELD_SOURCE
WHERE TRIM(rf.RDB$RELATION_NAME) = ?
ORDER BY rf.RDB$FIELD_POSITION
""".strip(),
    "select_max_pred_det_nr": """
SELECT COALESCE(MAX(NR), 0)
FROM PRED_DET
""".strip(),
    "select_max_bon_det_id_u": """
SELECT COALESCE(MAX(ID_U), 0)
FROM BON_DET
""".strip(),
    "select_articol_details_by_cod": """
SELECT FIRST 1 DENUMIRE, UM
FROM ARTICOLE
WHERE COD = ?
   OR TRIM(COD) = TRIM(?)
ORDER BY COD
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
    "insert_miscari_consum_bc_with_id_u": """
INSERT INTO MISCARI (
    ID,
    ID_U,
    DATA,
    NR_DOC,
    TIP_DOC,
    COD_ART,
    CANTITATE,
    GESTIUNE
) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
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
    "insert_miscari_produs_bp_with_pret_and_id_u": """
INSERT INTO MISCARI (
    ID,
    ID_U,
    DATA,
    NR_DOC,
    TIP_DOC,
    COD_ART,
    CANTITATE,
    GESTIUNE,
    PRET
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
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
    "insert_miscari_produs_bp_without_pret_and_id_u": """
INSERT INTO MISCARI (
    ID,
    ID_U,
    DATA,
    NR_DOC,
    TIP_DOC,
    COD_ART,
    CANTITATE,
    GESTIUNE
) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
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


def _operation_sign(pachet: PachetInput) -> int:
    """Return +1 for normal production, -1 for storno/desfacere productie."""
    return -1 if pachet.cantitate_produsa < 0 else 1


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
    if pachet.cantitate_produsa == 0:
        raise ValueError("pachet.cantitate_produsa must be non-zero.")
    if pachet.pret_vanz < 0:
        raise ValueError("pachet.pret_vanz must be >= 0.")
    if pachet.cost_total == 0:
        raise ValueError("pachet.cost_total must be non-zero.")
    if _quantize_money(pachet.cota_tva) not in _VALID_TVA:
        raise ValueError("pachet.cota_tva must be one of: 0, 11, 21.")

    expected_sign = 1 if pachet.cantitate_produsa > 0 else -1

    produse: list[ProdusInput] = []
    total_val_produse = Decimal("0")
    for index, produs_raw in enumerate(produse_raw, start=1):
        if not isinstance(produs_raw, dict):
            raise ValueError(f"produse[{index}] must be an object.")

        cantitate = _to_decimal(produs_raw.get("cantitate"), f"produse[{index}].cantitate")
        if cantitate == 0:
            raise ValueError(f"produse[{index}].cantitate must be non-zero.")
        if (1 if cantitate > 0 else -1) != expected_sign:
            raise ValueError(
                f"produse[{index}].cantitate sign must match pachet.cantitate_produsa sign."
            )

        val_produse = _to_decimal(produs_raw.get("val_produse"), f"produse[{index}].val_produse")
        if val_produse == 0:
            raise ValueError(f"produse[{index}].val_produse must be non-zero.")

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

    if (
        _quantize_money(total_val_produse) != _quantize_money(pachet.cost_total)
        and _quantize_money(abs(total_val_produse)) != _quantize_money(abs(pachet.cost_total))
    ):
        raise ValueError(
            "pachet.cost_total does not match SUM(produse[*].val_produse). "
            f"Expected {_quantize_money(total_val_produse)}, got {_quantize_money(pachet.cost_total)}."
        )

    return ProducePachetInput(pachet=pachet, produse=produse)


def ensurePachetInArticole(cursor: Any, pachet: PachetInput, allow_create: bool = True) -> str:
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

    if not allow_create:
        raise RuntimeError(
            "Package article does not exist in ARTICOLE for storno/desfacere production "
            f"(denumire='{pachet.denumire}')."
        )

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


def _find_existing_document(cursor: Any, pachet: PachetInput) -> dict[str, Any] | None:
    """
    Detect already imported document for same (ID_DOC, DATA).

    Returns existing nr_doc and pachet code when found.
    Raises if document looks partially imported (only BC or only BP).
    """
    # MISCARI.ID is generated as MAX(ID)+1.
    # For idempotency we first try PRED_DET.ID_DOC (payload id_doc), then
    # fall back to legacy rows where ID_UNIC previously stored payload id_doc.
    pred_row = None
    try:
        cursor.execute(SQL_QUERIES["select_pred_det_existing_nr_doc_by_id_doc"], [pachet.id_doc])
        pred_row = cursor.fetchone()
    except Exception:
        pred_row = None
    if not pred_row:
        # Backward compatibility for older imports where ID_UNIC was mapped to payload id_doc.
        try:
            cursor.execute(SQL_QUERIES["select_pred_det_existing_nr_doc_by_id_unic"], [pachet.id_doc])
            pred_row = cursor.fetchone()
        except Exception:
            pred_row = None

    if not pred_row:
        return None

    nr_doc_existing = int(pred_row[0] or 0)
    if nr_doc_existing <= 0:
        return None

    cursor.execute(
        SQL_QUERIES["select_doc_counts_by_date_nr_doc"],
        [pachet.data, nr_doc_existing],
    )
    bc_count, bp_count = cursor.fetchone()
    bc_count_int = int(bc_count or 0)
    bp_count_int = int(bp_count or 0)

    if bc_count_int == 0 and bp_count_int == 0:
        return None
    if bc_count_int == 0 or bp_count_int == 0:
        raise RuntimeError(
            "Existing production document is inconsistent "
            f"(id_doc={pachet.id_doc}, data={pachet.data}, BC={bc_count_int}, BP={bp_count_int})."
        )

    cursor.execute(
        SQL_QUERIES["select_bp_doc_by_date_nr_doc"],
        [pachet.data, nr_doc_existing],
    )
    bp_row = cursor.fetchone()
    if not bp_row:
        raise RuntimeError(
            "Existing production document was found in PRED_DET but BP row is missing in MISCARI "
            f"(id_doc={pachet.id_doc}, data={pachet.data}, nr_doc={nr_doc_existing})."
        )

    miscari_id_existing = int(bp_row[0] or 0)
    cod_pachet_db = str(bp_row[1] or "")
    if cod_pachet_db and len(cod_pachet_db) < 16:
        cod_pachet_db = cod_pachet_db.strip().ljust(16)

    return {
        "miscari_id": miscari_id_existing,
        "nr_doc": nr_doc_existing,
        "cod_pachet_db": cod_pachet_db[:16] if cod_pachet_db else "",
        "bc_count": bc_count_int,
        "bp_count": bp_count_int,
    }


def _miscari_has_pret_column(cursor: Any) -> bool:
    cursor.execute(SQL_QUERIES["check_miscari_has_pret_column"], ["MISCARI", "PRET"])
    return int(cursor.fetchone()[0] or 0) > 0


def _miscari_has_id_u_column(cursor: Any) -> bool:
    cursor.execute(SQL_QUERIES["check_miscari_has_pret_column"], ["MISCARI", "ID_U"])
    return int(cursor.fetchone()[0] or 0) > 0


def _get_next_miscari_id_u(cursor: Any) -> int:
    cursor.execute(SQL_QUERIES["select_max_miscari_id_u"])
    current_max = int(cursor.fetchone()[0] or 0)
    return current_max + 1


def _get_next_miscari_id(cursor: Any) -> int:
    cursor.execute(SQL_QUERIES["select_max_miscari_id"])
    current_max = int(cursor.fetchone()[0] or 0)
    return current_max + 1


def _get_next_pred_det_nr(cursor: Any) -> int:
    cursor.execute(SQL_QUERIES["select_max_pred_det_nr"])
    current_max = int(cursor.fetchone()[0] or 0)
    return current_max + 1


def _get_next_bon_det_id_u(cursor: Any) -> int:
    cursor.execute(SQL_QUERIES["select_max_bon_det_id_u"])
    current_max = int(cursor.fetchone()[0] or 0)
    return current_max + 1


def _get_articol_consum_details(cursor: Any, cod_articol_db: str) -> tuple[str, str]:
    cursor.execute(SQL_QUERIES["select_articol_details_by_cod"], [cod_articol_db, cod_articol_db])
    row = cursor.fetchone()
    if not row:
        raise RuntimeError(
            "Cannot insert BON_DET consumption line because product was not found in ARTICOLE "
            f"(cod_articol={_trim_db_char(cod_articol_db)})."
        )

    denumire = _trim_db_char(str(row[0] or ""))
    um = _trim_db_char(str(row[1] or ""))
    if not denumire:
        denumire = _trim_db_char(cod_articol_db)
    if not um:
        um = "BUC"
    return denumire, um


def _trim_db_char(value: str) -> str:
    return str(value or "").rstrip()


def _build_dynamic_insert_sql(table_name: str, columns: list[str]) -> str:
    placeholders = ", ".join(["?"] * len(columns))
    column_list = ", ".join(columns)
    return f"INSERT INTO {table_name} ({column_list}) VALUES ({placeholders})"


def _get_relation_fields(cursor: Any, relation_name: str) -> list[dict[str, Any]]:
    cursor.execute(SQL_QUERIES["select_relation_fields"], [relation_name])
    fields: list[dict[str, Any]] = []
    for field_name, null_flag, default_source, identity_type, field_type in cursor.fetchall():
        fields.append(
            {
                "name": str(field_name),
                "required": int(null_flag or 0) == 1 and default_source is None,
                "identity": int(identity_type) >= 0,
                "field_type": int(field_type or 0),
            }
        )
    return fields


def _default_value_for_field_type(field_type: int, pachet: PachetInput) -> Any | None:
    if field_type in {7, 8, 10, 16, 23, 27}:  # numeric + boolean
        return 0
    if field_type == 12:  # DATE
        return pachet.data
    if field_type == 13:  # TIME
        return dt_time(0, 0, 0)
    if field_type == 35:  # TIMESTAMP
        return datetime.combine(pachet.data, dt_time(0, 0, 0))
    if field_type in {14, 37}:  # CHAR, VARCHAR
        return ""
    return None


def _pred_det_field_value(
    field_name: str,
    pachet: PachetInput,
    first_produs: ProdusInput,
    cod_pachet_db: str,
    miscari_doc_id: int,
    nr_doc: int,
    pred_det_nr: int | None,
    line_no: int,
) -> Any | None:
    upper = field_name.upper()
    sign = _operation_sign(pachet)
    qty = Decimal(sign) * abs(pachet.cantitate_produsa)
    sale_value = Decimal(sign) * abs(pachet.pret_vanz)
    cost_total_value = Decimal(sign) * abs(pachet.cost_total)
    unit_price = sale_value

    direct_values = {
        "ID_DOC": pachet.id_doc,
        "IDDOC": pachet.id_doc,
        "DOC_ID": pachet.id_doc,
        "ID_DOCUMENT": pachet.id_doc,
        "ID": miscari_doc_id,
        "ID_UNIC": miscari_doc_id,
        "IDUNIC": miscari_doc_id,
        "VALIDAT": "V",
        "DATA": pachet.data,
        "DATA_DOC": pachet.data,
        "NR_DOC": nr_doc,
        "NR": pred_det_nr,
        "TIP_DOC": "BP",
        "TIPDOC": "BP",
        "GESTIUNE": pachet.gestiune,
        "GEST": pachet.gestiune,
        "DEN_GEST": "GESTIUNEA 1",
        "DENGEST": "GESTIUNEA 1",
        "DENGESTIUNE": "GESTIUNEA 1",
        "DEN_TIP": "Produse finite",
        "DENTIP": "Produse finite",
        "UM": "BUC",
        "UNITATE_MASURA": "BUC",
        "UNITATE": "BUC",
        "CANTITATE": qty,
        "CANT": qty,
        "QTY": qty,
        "VALOARE": sale_value,
        "VAL": sale_value,
        "COST": cost_total_value,
        "COST_TOTAL": cost_total_value,
        "VAL_CONSUM": cost_total_value,
        "VAL_CONSUMURI": cost_total_value,
        "TOTAL_CONSUM": cost_total_value,
        "CONSUM": cost_total_value,
        "PRET": unit_price,
        "PRET_VANZ": unit_price,
        "PRET_UNITAR": unit_price,
        "COST_UNITAR": unit_price,
        "TVA": pachet.cota_tva,
        "COTA_TVA": pachet.cota_tva,
        "DENUMIRE": pachet.denumire,
        "DEN_ART": pachet.denumire,
        "NR_POZ": line_no,
        "POZITIE": line_no,
        "NR_LINIE": line_no,
        "LINIE": line_no,
    }
    if upper in direct_values:
        return direct_values[upper]

    if "COD" in upper:
        if any(key in upper for key in ("COMP", "MAT", "MATER", "MP")):
            return first_produs.cod_articol_db
        if any(key in upper for key in ("PACH", "PF", "PRODUS")):
            return cod_pachet_db
        if upper in {"COD_ARTICOL", "COD_ART"}:
            return cod_pachet_db
        return cod_pachet_db

    if "CONSUM" in upper or upper.endswith("_COST") or upper.startswith("COST_"):
        return cost_total_value

    return None


def _bon_det_field_value(
    *,
    field_name: str,
    pachet: PachetInput,
    produs: ProdusInput,
    miscari_doc_id: int,
    nr_doc: int,
    line_no: int,
    bon_det_id_u: int | None,
    qty_consum: Decimal,
    unit_price: Decimal,
    line_value: Decimal,
    produs_denumire: str,
    produs_um: str,
) -> Any | None:
    upper = field_name.upper()
    direct_values = {
        "ID": miscari_doc_id,
        "ID_UNIC": miscari_doc_id,
        "IDUNIC": miscari_doc_id,
        "ID_DOC": pachet.id_doc,
        "IDDOC": pachet.id_doc,
        "ID_U": bon_det_id_u,
        "IDU": bon_det_id_u,
        "VALIDAT": "V",
        "DATA": pachet.data,
        "DATA_DOC": pachet.data,
        "NR_DOC": nr_doc,
        "TIP_DOC": "BC",
        "TIPDOC": "BC",
        "GESTIUNE": "Gestiunea 1",
        "GEST": "Gestiunea 1",
        "DEN_GEST": "Gestiunea 1",
        "DENGEST": "Gestiunea 1",
        "DENGESTIUNE": "Gestiunea 1",
        "COD": produs.cod_articol_db,
        "COD_ART": produs.cod_articol_db,
        "CODART": produs.cod_articol_db,
        "COD_ARTICOL": produs.cod_articol_db,
        "DENUMIRE": produs_denumire,
        "DEN_ART": produs_denumire,
        "DENART": produs_denumire,
        "UM": produs_um,
        "UNITATE_MASURA": produs_um,
        "UNITATE": produs_um,
        "DEN_TIP": "Materii prime",
        "DENTIP": "Materii prime",
        "CANTITATE": qty_consum,
        "CANT": qty_consum,
        "QTY": qty_consum,
        "PRET": unit_price,
        "PRET_UNITAR": unit_price,
        "PRET_MEDIU": unit_price,
        "VALOARE": line_value,
        "VAL": line_value,
        "COST": line_value,
        "COST_TOTAL": line_value,
        "IS_PROD": 1,
        "ISPROD": 1,
        "NR_POZ": line_no,
        "POZITIE": line_no,
        "NR_LINIE": line_no,
        "LINIE": line_no,
    }
    if upper in direct_values:
        return direct_values[upper]

    if "COD" in upper:
        return produs.cod_articol_db
    if "GEST" in upper:
        return "Gestiunea 1"
    if ("DEN" in upper and "TIP" in upper) or "MATER" in upper:
        return "Materii prime"
    if upper in {"DENUMIRE_ARTICOL", "NUME_ARTICOL"}:
        return produs_denumire
    if upper in {"UNITATE_DE_MASURA", "U_M"}:
        return produs_um
    if "CANT" in upper or upper == "QTY":
        return qty_consum
    if upper.startswith("PRET"):
        return unit_price
    if "VAL" in upper or "COST" in upper:
        return line_value
    if "PROD" in upper and "IS" in upper:
        return 1
    if upper.endswith("ID_U") or upper == "ID_U":
        return bon_det_id_u

    return None


def _insert_bon_det_rows(
    *,
    cursor: Any,
    request: ProducePachetInput,
    miscari_doc_id: int,
    nr_doc: int,
) -> dict[str, int | None]:
    fields = _get_relation_fields(cursor, "BON_DET")
    if not fields:
        raise RuntimeError(
            "Cannot insert BON_DET rows: table BON_DET has no readable columns in current schema."
        )

    insertable_fields = [field for field in fields if not field["identity"]]
    if not insertable_fields:
        raise RuntimeError("Cannot insert BON_DET rows: no writable BON_DET columns were found.")

    has_id_u_column = any(field["name"].upper() == "ID_U" for field in insertable_fields)
    next_id_u = _get_next_bon_det_id_u(cursor) if has_id_u_column else None
    id_u_start = next_id_u if next_id_u is not None else None
    inserted_rows = 0
    pachet = request.pachet
    is_storno = _operation_sign(pachet) < 0
    articol_cache: dict[str, tuple[str, str]] = {}

    for line_no, produs in enumerate(request.produse, start=1):
        qty_consum = abs(produs.cantitate) if is_storno else -abs(produs.cantitate)
        qty_abs = abs(produs.cantitate)
        line_value_sign = Decimal(1) if qty_consum >= 0 else Decimal(-1)
        line_value = line_value_sign * abs(produs.val_produse)
        unit_price = Decimal("0")
        if qty_abs != 0:
            unit_price = _quantize_money(abs(produs.val_produse) / qty_abs)

        cod_key = produs.cod_articol_db
        if cod_key not in articol_cache:
            articol_cache[cod_key] = _get_articol_consum_details(cursor, cod_key)
        produs_denumire, produs_um = articol_cache[cod_key]

        current_id_u = int(next_id_u) if next_id_u is not None else None
        if next_id_u is not None:
            next_id_u = current_id_u + 1

        columns: list[str] = []
        values: list[Any] = []
        missing_required: list[str] = []
        for field in insertable_fields:
            field_name = field["name"]
            value = _bon_det_field_value(
                field_name=field_name,
                pachet=pachet,
                produs=produs,
                miscari_doc_id=miscari_doc_id,
                nr_doc=nr_doc,
                line_no=line_no,
                bon_det_id_u=current_id_u,
                qty_consum=qty_consum,
                unit_price=unit_price,
                line_value=line_value,
                produs_denumire=produs_denumire,
                produs_um=produs_um,
            )
            if value is None and field["required"]:
                value = _default_value_for_field_type(field["field_type"], pachet)
            if value is None:
                if field["required"]:
                    missing_required.append(field_name)
                continue

            columns.append(field_name)
            values.append(value)

        if missing_required:
            missing_list = ", ".join(sorted(missing_required))
            raise RuntimeError(
                "Cannot insert BON_DET line "
                f"{line_no}. Missing required mapped columns: {missing_list}"
            )
        if not columns:
            raise RuntimeError(
                f"Cannot insert BON_DET line {line_no}. No compatible columns were mapped."
            )

        cursor.execute(_build_dynamic_insert_sql("BON_DET", columns), values)
        inserted_rows += 1

    id_u_end = (int(next_id_u) - 1) if next_id_u is not None else None
    return {
        "inserted": inserted_rows,
        "id_u_start": id_u_start,
        "id_u_end": id_u_end,
    }


def _insert_pred_det_rows(
    cursor: Any,
    request: ProducePachetInput,
    cod_pachet_db: str,
    miscari_doc_id: int,
    nr_doc: int,
) -> int:
    fields = _get_relation_fields(cursor, "PRED_DET")
    if not fields:
        return 0

    insertable_fields = [field for field in fields if not field["identity"]]
    if not insertable_fields:
        return 0

    inserted_rows = 0
    pachet = request.pachet

    first_produs = request.produse[0]
    line_no = 1
    has_nr_column = any(field["name"].upper() == "NR" for field in insertable_fields)
    pred_det_nr = _get_next_pred_det_nr(cursor) if has_nr_column else None
    columns: list[str] = []
    values: list[Any] = []
    missing_required: list[str] = []

    for field in insertable_fields:
        field_name = field["name"]
        value = _pred_det_field_value(
            field_name=field_name,
            pachet=pachet,
            first_produs=first_produs,
            cod_pachet_db=cod_pachet_db,
            miscari_doc_id=miscari_doc_id,
            nr_doc=nr_doc,
            pred_det_nr=pred_det_nr,
            line_no=line_no,
        )
        if value is None and field["required"]:
            value = _default_value_for_field_type(field["field_type"], pachet)
        if value is None:
            if field["required"]:
                missing_required.append(field_name)
            continue

        columns.append(field_name)
        values.append(value)

    if missing_required:
        missing_list = ", ".join(sorted(missing_required))
        raise RuntimeError(
            "Cannot insert into PRED_DET. Missing required mapped columns: "
            f"{missing_list}"
        )
    if not columns:
        raise RuntimeError("Cannot insert into PRED_DET. No compatible columns were mapped.")

    cursor.execute(_build_dynamic_insert_sql("PRED_DET", columns), values)
    inserted_rows += 1

    return inserted_rows


def _execute_produce_pachet_once(cursor: Any, request: ProducePachetInput) -> dict[str, Any]:
    pachet = request.pachet
    is_storno = _operation_sign(pachet) < 0
    existing = _find_existing_document(cursor, pachet)
    if existing is not None:
        cod_pachet_db = str(existing.get("cod_pachet_db") or "")
        if not cod_pachet_db:
            # Fallback if legacy rows did not have usable COD_ART.
            cod_pachet_db = ensurePachetInArticole(cursor, pachet)
        return {
            "success": True,
            "message": "producePachet skipped: document already imported.",
            "codPachet": _trim_db_char(cod_pachet_db),
            "nrDoc": int(existing["nr_doc"]),
            "idDoc": pachet.id_doc,
            "miscariId": int(existing.get("miscari_id") or 0),
            "bonDetInserted": 0,
            "predDetInserted": 0,
            "alreadyImported": True,
            "idUStart": None,
            "idUEnd": None,
            "bonDetIdUStart": None,
            "bonDetIdUEnd": None,
        }

    cod_pachet_db = ensurePachetInArticole(cursor, pachet, allow_create=not is_storno)
    miscari_doc_id = _get_next_miscari_id(cursor)
    nr_doc = getNextNrDoc(cursor, pachet.data)
    miscari_has_pret = _miscari_has_pret_column(cursor)
    miscari_has_id_u = _miscari_has_id_u_column(cursor)
    next_id_u = _get_next_miscari_id_u(cursor) if miscari_has_id_u else None
    id_u_start = next_id_u if next_id_u is not None else None

    qty_produs = abs(pachet.cantitate_produsa)
    qty_bp = -qty_produs if is_storno else qty_produs
    # Business order requested: BC rows first, then BP row.
    for produs in request.produse:
        qty_consum = abs(produs.cantitate) if is_storno else -abs(produs.cantitate)
        if miscari_has_id_u:
            current_id_u = int(next_id_u)
            next_id_u = current_id_u + 1
            cursor.execute(
                SQL_QUERIES["insert_miscari_consum_bc_with_id_u"],
                [
                    miscari_doc_id,
                    current_id_u,
                    pachet.data,
                    nr_doc,
                    "BC",
                    produs.cod_articol_db,
                    qty_consum,
                    pachet.gestiune,
                ],
            )
        else:
            cursor.execute(
                SQL_QUERIES["insert_miscari_consum_bc"],
                [
                    miscari_doc_id,
                    pachet.data,
                    nr_doc,
                    "BC",
                    produs.cod_articol_db,
                    qty_consum,
                    pachet.gestiune,
                ],
            )

    if miscari_has_pret and miscari_has_id_u:
        current_id_u = int(next_id_u)
        next_id_u = current_id_u + 1
        cursor.execute(
            SQL_QUERIES["insert_miscari_produs_bp_with_pret_and_id_u"],
            [
                miscari_doc_id,
                current_id_u,
                pachet.data,
                nr_doc,
                "BP",
                cod_pachet_db,
                qty_bp,
                pachet.gestiune,
                pachet.pret_vanz,
            ],
        )
    elif miscari_has_pret:
        cursor.execute(
            SQL_QUERIES["insert_miscari_produs_bp_with_pret"],
            [
                miscari_doc_id,
                pachet.data,
                nr_doc,
                "BP",
                cod_pachet_db,
                qty_bp,
                pachet.gestiune,
                pachet.pret_vanz,
            ],
        )
    elif miscari_has_id_u:
        current_id_u = int(next_id_u)
        next_id_u = current_id_u + 1
        cursor.execute(
            SQL_QUERIES["insert_miscari_produs_bp_without_pret_and_id_u"],
            [
                miscari_doc_id,
                current_id_u,
                pachet.data,
                nr_doc,
                "BP",
                cod_pachet_db,
                qty_bp,
                pachet.gestiune,
            ],
        )
    else:
        cursor.execute(
            SQL_QUERIES["insert_miscari_produs_bp_without_pret"],
            [
                miscari_doc_id,
                pachet.data,
                nr_doc,
                "BP",
                cod_pachet_db,
                qty_bp,
                pachet.gestiune,
            ],
        )

    bon_det_result = _insert_bon_det_rows(
        cursor=cursor,
        request=request,
        miscari_doc_id=miscari_doc_id,
        nr_doc=nr_doc,
    )
    pred_det_inserted = _insert_pred_det_rows(
        cursor=cursor,
        request=request,
        cod_pachet_db=cod_pachet_db,
        miscari_doc_id=miscari_doc_id,
        nr_doc=nr_doc,
    )
    id_u_end = (int(next_id_u) - 1) if next_id_u is not None else None

    return {
        "success": True,
        "message": "producePachet executed successfully.",
        "codPachet": _trim_db_char(cod_pachet_db),
        "nrDoc": nr_doc,
        "idDoc": pachet.id_doc,
        "miscariId": miscari_doc_id,
        "bonDetInserted": int(bon_det_result["inserted"] or 0),
        "predDetInserted": pred_det_inserted,
        "alreadyImported": False,
        "idUStart": id_u_start,
        "idUEnd": id_u_end,
        "bonDetIdUStart": bon_det_result["id_u_start"],
        "bonDetIdUEnd": bon_det_result["id_u_end"],
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

