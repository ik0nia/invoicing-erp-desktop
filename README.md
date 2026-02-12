# Desktop Stock ERP Integration (Windows)

Aplicatie desktop simpla pentru Windows, facuta in Python + Tkinter, care:

- se conecteaza la Firebird 3
- ruleaza periodic comenzi SQL de tip INSERT venite din API
- ruleaza periodic un SELECT de stocuri
- exporta rezultatul in CSV
- trimite CSV-ul catre un endpoint PHP pentru incarcare automata in ERP

## De ce aceasta varianta?

Pentru cerinta ta, varianta cea mai rapida si simpla de implementat/mentenanta este:

- **Python** (rapid de dezvoltat)
- **Tkinter** (UI desktop nativ, fara framework greu)
- **firebird-driver** pentru Firebird 3
- **requests** pentru API-uri HTTP

Ulterior se poate impacheta in `.exe` cu PyInstaller.

---

## Structura proiect

- `desktop_stock_erp_app.py` - aplicatia desktop
- `requirements.txt` - dependinte Python
- `examples/upload_stock.php` - exemplu endpoint PHP pentru upload CSV
- `config.example.json` - exemplu de configurare initiala
- `config.json` - se genereaza automat la salvare din aplicatie

---

## Instalare (dezvoltare)

1. Instaleaza Python 3.11+ pe Windows.
2. Din folderul proiectului:

```bash
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
```

3. Ruleaza aplicatia:

```bash
python desktop_stock_erp_app.py
```

---

## Configurare in aplicatie

### 1) Tab Firebird

- **Database file (.fdb)**: alegi fisierul bazei
- **Host**: lasa gol pentru conexiune directa la fisier (`embedded`); completeaza doar daca folosesti server Firebird
- **Port**: `3050` (conteaza doar cand Host este setat)
- **User/Password**: ex. `SYSDBA` / `masterkey`
- **Charset**: ex. `UTF8`
- **Firebird client library (fbclient.dll)**: optional, dar recomandat daca aplicatia nu gaseste automat clientul Firebird
- **Start minimized in system tray (near clock)**: porneste aplicatia direct in tray; deschiderea se face din iconita

### 2) Tab Import Pachete Saga

- **Enable Import Pachete Saga job**: activeaza jobul de import periodic
- **Import API URL**: endpoint de unde aplicatia citeste pachetele de importat (`producePachet`)
- **Import API token**: optional (Bearer token)
- **Import token query param**: daca API cere token in query string, ex. `token`
- **Import headers JSON**: headere custom pentru import (ex: `{"X-Api-Key":"abc"}`)
- **Import User-Agent**: util cand API-ul blocheaza user-agent-ul default
- **Status update API URL (optional)**: dupa import reusit, aplicatia apeleaza acest endpoint
- **Status update ID param (optional)**: numele parametrului pentru identificator (ex: `id_pachet`, `id_doc`, `nr_doc`)
- **Import interval (seconds)**: la cat timp sa ruleze
- **Import schedule start / end (HH:MM, optional)**: fereastra orara in care jobul de import are voie sa ruleze

Payload suportat:

```json
{
  "pachet": {
    "id_doc": 3457,
    "data": "2026-02-10",
    "denumire": "PACHET #1010",
    "pret_vanz": 500.0,
    "cota_tva": 21.0,
    "cost_total": 120.0,
    "gestiune": "0001",
    "cantitate_produsa": 1.0,
    "status": "processing"
  },
  "produse": [
    { "cod_articol": "00000402", "cantitate": 3.0, "val_produse": 60.0 }
  ]
}
```

Exemplu pentru endpoint care cere token in query:

- Import API URL: `https://deon.ro/erp/api/pachete/import`
- Import API token: `ParolaToken123!`
- Import token query param: `token`

Exemplu callback status dupa import:

- Status update API URL: `https://deon.ro/erp/api/pachete/update-status`
- Status update ID param: `id_pachet`
- Aplicatia va apela: `...?id_pachet=<valoare>&status=imported&token=ParolaToken123!`
- Valoarea trimisa este aleasa in ordinea: `id_pachet` -> `id_doc` -> `nr_doc`
- Token-ul/parametrul de token este acelasi ca la import (`Import API token` + `Import token query param`).

Sau lista:

```json
{
  "pachete": [
    { "pachet": { "...": "..." }, "produse": [ { "...": "..." } ] }
  ]
}
```

### 3) Tab Stock export

- **Enable periodic stock export job**: activeaza exportul periodic
- **Export interval (seconds)**: la cat timp ruleaza query-ul de stoc
- **Export schedule start / end (HH:MM, optional)**: fereastra orara in care jobul de export are voie sa ruleze
- **Upload URL**: endpoint PHP care primeste CSV
- **Upload file field name**: implicit `file`
- **Upload API token**: token separat pentru upload (optional; daca e gol se foloseste token-ul din Import Pachete Saga)
- **Upload token query param**: daca API vrea token in query string, ex. `token`
- **Upload headers JSON**: headere custom pentru upload (ex: `{"X-App-Key":"abc"}`)
- **Upload User-Agent**: util cand serverul blocheaza `python-requests/*`
- **CSV directory**: folder local unde salveaza fisierele CSV
- **Audit log directory**: folder local pentru jurnalul upload-urilor (fisier `.jsonl`)
- **HTTP timeout**: timeout request-uri
- **Verify SSL**: validare certificat HTTPS
- **Extra upload fields JSON**: campuri suplimentare trimise in POST
- **Stock SELECT SQL**: query-ul de stoc ce va fi exportat

Nota: la export CSV, valorile text sunt curatate de padding-ul din dreapta (spatii/tab-uri), util pentru coloane Firebird de tip `CHAR`.

Daca primesti 403 la upload, verifica:
- endpoint-ul accepta POST multipart (nu doar GET in browser)
- tokenul/header-ele cerute de server
- `Upload file field name` (sa fie exact cum asteapta serverul, ex: `file` sau `csv`)
- User-Agent (unele servere blocheaza requests default)

Exemplu mapare pentru comanda:
`curl -X POST "https://deon.ro/erp/api/stock/import?token=ParolaToken123!" -F "stock_csv=@/path/stoc.csv"`

- Upload URL: `https://deon.ro/erp/api/stock/import`
- Upload file field name: `stock_csv`
- Upload API token: `ParolaToken123!`
- Upload token query param: `token`

Aplicatia afiseaza acum in log si corpul raspunsului serverului la erori HTTP (util pentru debug).
La upload reusit, logul afiseaza si raspunsul API (JSON/text, trunchiat daca e foarte lung).
In plus, fiecare upload (success/error) este salvat local in `audit_logs/upload_audit_YYYYMMDD.jsonl`
(sau in folderul setat la `audit_log_directory`).

Exemplu query:

```sql
SELECT SKU, QTY_AVAILABLE
FROM STOCKS
WHERE ACTIVE = 1
```

---

## Flux de lucru

1. Apesi **Save config**
2. Apesi **Start scheduler**
3. Aplicatia ruleaza in bucla:
   - ia pachetele `processing` din API si executa `producePachet` in Firebird
  - verifica imediat in DB ca exista liniile inserate in `MISCARI` (BC/BP), `BON_DET` (consumuri) si `PRED_DET` pentru `id_doc + data + nr_doc`
   - pentru fiecare import reusit, apeleaza API-ul de status cu `<id_param>=valoare` si `status=imported` (daca este setat)
   - ruleaza select-ul de stoc, face CSV si upload la PHP
4. Poti testa manual cu:
   - **Run import now**
   - **Run export now**

Toate actiunile apar in tab-ul **Logs** (culori pe severitate, scrollbar orizontal/vertical, export in fisier).

---

## Exemplu endpoint PHP

Ai un exemplu in:

- `examples/upload_stock.php`

Acesta salveaza CSV-ul intr-un folder local `uploads/` si returneaza JSON.

---

## Build executabil Windows (.exe)

### Varianta usoara (recomandat)

In radacina proiectului ruleaza direct:

```bat
build_exe.bat
```

Scriptul creeaza automat `.venv`, instaleaza dependintele si face build.
Executabilul rezultat va fi in:

- `dist\DesktopStockErpIntegration\DesktopStockErpIntegration.exe`

### Varianta manuala

Instaleaza PyInstaller:

```bash
pip install pyinstaller
```

Genereaza exe:

```bash
pyinstaller --noconfirm --clean --onedir --windowed --name DesktopStockErpIntegration --hidden-import=pystray._win32 --collect-all pystray --collect-all PIL desktop_stock_erp_app.py
```

---

## Observatii importante pentru Firebird 3

- Pe PC-ul unde ruleaza aplicatia trebuie sa existe clientul Firebird (`fbclient.dll`).
- Daca folosesti server Firebird, verifica accesul la baza si firewall-ul pentru portul 3050.
- Pentru embedded/local lasa host gol si selecteaza direct fisierul `.fdb`.
- Daca apare eroarea "client library could not be determined", seteaza explicit calea la `fbclient.dll` in tab-ul Firebird.

---

## Functia `producePachet` (tranzactie unica, fara EXECUTE BLOCK)

Am adaugat implementarea in:

- `produce_pachet_service.py`

Functia principala:

- `producePachet(payload, db_settings)`

Caracteristici:

- validare input JSON (campuri obligatorii + tipuri)
- query-uri parametrizate (`?`) pentru toate operatiile
- o singura tranzactie (`COMMIT` la succes, `ROLLBACK` la eroare)
- `ID_DOC` din payload este pastrat pentru legatura externa (status/API)
- daca exista coloana `ID_U` in `MISCARI`, este setata incremental (`MAX(ID_U)+1`) pentru fiecare linie noua BC/BP
- coloana `ID` din `MISCARI` este alocata ca `MAX(ID)+1` pentru document, iar liniile `BC` si `BP` folosesc acelasi `ID`
- `NR_DOC` este calculat `MAX(NR_DOC)+1` pe aceeasi data pentru `TIP_DOC='BP'`
- insereaza consumurile/produsul finit in `MISCARI`, consumurile in `BON_DET` (cate 1 rand pentru fiecare linie BC) si 1 rand de productie in `PRED_DET` (daca tabelul exista)
- ordinea inserarii in `MISCARI` este: intai liniile `BC`, apoi `BP`
- la inserarea pachetului in `ARTICOLE`, `PRET_VANZ` si `PRET_V_TVA` nu sunt setate
- in `PRED_DET`: `ID` si `ID_UNIC` folosesc `ID`-ul documentului din `MISCARI`, `VALIDAT = 'V'`, `NR = MAX(NR)+1`, `DEN_GEST = 'GESTIUNEA 1'`, `DEN_TIP = 'Produse finite'`, `PRET/VALOARE = pret_vanz`, iar campurile de consum/cost primesc `cost_total`
- in `BON_DET`: `ID` foloseste acelasi `ID` de document din `MISCARI` (si `ID_UNIC` din `PRED_DET`), `ID_U` este incremental (`MAX(ID_U)+1`), `VALIDAT = 'V'`, `GESTIUNE = 'Gestiunea 1'`, `DEN_TIP = 'Materii prime'`, `IS_PROD = 1`, iar `CANTITATE` are acelasi semn ca linia `BC`; `PRET` si `VALOARE` se completeaza din datele produsului
- cazul storno/desfacere productie (cantitati negative):
  - `BP` se insereaza cu cantitate negativa
  - liniile `BC` se insereaza cu cantitate pozitiva (reintrare in stoc)
  - in `PRED_DET` operatiunea este cu semn minus
  - produsul pachet trebuie sa existe deja in `ARTICOLE` (nu se creeaza automat)
- evita dublurile: daca documentul exista deja pentru acelasi `id_doc + data`, nu mai insereaza inca o data
- concurenta minima:
  - duplicate la inserarea in `ARTICOLE` -> recitire dupa `DENUMIRE`
  - conflict unic la miscari/nr_doc -> retry o singura data

Helper-ele cerute:

- `normalizeCodArticol(cod)`
- `ensurePachetInArticole(cursor, pachet)`
- `getNextNrDoc(cursor, data_doc)`

Input exemplu:

- `examples/produce_pachet_payload.example.json`

Exemplu de folosire:

```python
import json
from pathlib import Path

from produce_pachet_service import (
    FirebirdConnectionSettings,
    get_produce_pachet_sql_queries,
    producePachet,
    validate_produce_pachet_input,
)

payload = json.loads(Path("examples/produce_pachet_payload.example.json").read_text(encoding="utf-8"))

# optional: doar validare
validated = validate_produce_pachet_input(payload)

db_settings = FirebirdConnectionSettings(
    database_path=r"C:\SAGA 3.0\company.fdb",
    host="",
    port=3050,
    user="SYSDBA",
    password="masterkey",
    charset="UTF8",
    fb_client_library_path=r"C:\Program Files\Firebird\Firebird_3_0\fbclient.dll",
)

result = producePachet(payload, db_settings)
print(result)
# {'success': True, 'message': 'producePachet executed successfully.', 'codPachet': '00001234', 'nrDoc': 145, 'idDoc': 3457}

sql_list = get_produce_pachet_sql_queries()
print(sql_list)
```

Rezultat JSON la succes:

```json
{
  "success": true,
  "message": "producePachet executed successfully.",
  "codPachet": "00001234",
  "nrDoc": 145,
  "idDoc": 3457
}
```
