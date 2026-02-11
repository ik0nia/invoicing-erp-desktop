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

### 2) Tab API sync

- **Enable periodic sync job**: activeaza jobul de insert periodic
- **Sync API URL**: endpoint care returneaza comenzile SQL
- **API token**: optional (Bearer token)
- **Sync interval (seconds)**: la cat timp sa ruleze

Payload-ul API suportat:

```json
[
  {
    "sql": "INSERT INTO STOCK_UPDATES(ID, SKU, QTY) VALUES (?, ?, ?)",
    "params": [1, "ABC-01", 15]
  }
]
```

Sau:

```json
{
  "commands": [
    {
      "sql": "INSERT INTO STOCK_UPDATES(ID, SKU, QTY) VALUES (?, ?, ?)",
      "params": [1, "ABC-01", 15]
    }
  ]
}
```

### 3) Tab Stock export

- **Enable periodic stock export job**: activeaza exportul periodic
- **Export interval (seconds)**: la cat timp ruleaza query-ul de stoc
- **Upload URL**: endpoint PHP care primeste CSV
- **Upload file field name**: implicit `file`
- **CSV directory**: folder local unde salveaza fisierele CSV
- **HTTP timeout**: timeout request-uri
- **Verify SSL**: validare certificat HTTPS
- **Extra upload fields JSON**: campuri suplimentare trimise in POST
- **Stock SELECT SQL**: query-ul de stoc ce va fi exportat

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
   - ia insert-uri din API si le executa in Firebird
   - ruleaza select-ul de stoc, face CSV si upload la PHP
4. Poti testa manual cu:
   - **Run sync now**
   - **Run export now**

Toate actiunile apar in tab-ul **Logs**.

---

## Exemplu endpoint PHP

Ai un exemplu in:

- `examples/upload_stock.php`

Acesta salveaza CSV-ul intr-un folder local `uploads/` si returneaza JSON.

---

## Build executabil Windows (.exe)

Instaleaza PyInstaller:

```bash
pip install pyinstaller
```

Genereaza exe:

```bash
pyinstaller --noconfirm --onefile --windowed desktop_stock_erp_app.py
```

Executabilul rezultat va fi in `dist/desktop_stock_erp_app.exe`.

---

## Observatii importante pentru Firebird 3

- Pe PC-ul unde ruleaza aplicatia trebuie sa existe clientul Firebird (`fbclient.dll`).
- Daca folosesti server Firebird, verifica accesul la baza si firewall-ul pentru portul 3050.
- Pentru embedded/local lasa host gol si selecteaza direct fisierul `.fdb`.
- Daca apare eroarea "client library could not be determined", seteaza explicit calea la `fbclient.dll` in tab-ul Firebird.
