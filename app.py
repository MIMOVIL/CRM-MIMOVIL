import os
import sqlite3
from datetime import datetime, timedelta, date
from functools import wraps

from flask import (
    Flask, g, redirect, render_template, request, session,
    url_for, flash, jsonify
)
from authlib.integrations.flask_client import OAuth

# =========================
# Config
# =========================
APP_SECRET = os.environ.get("APP_SECRET", "crm_mimovil_clave_larga_cambiar")
DATABASE = os.environ.get("DATABASE_PATH", "crm.sqlite")

GOOGLE_CLIENT_ID = os.environ.get("GOOGLE_CLIENT_ID")
GOOGLE_CLIENT_SECRET = os.environ.get("GOOGLE_CLIENT_SECRET")

ALLOWED_EMAILS = set(
    e.strip().lower()
    for e in os.environ.get("ALLOWED_EMAILS", "").split(",")
    if e.strip()
)

# días para alertar (puedes cambiarlo en Render como variable de entorno)
ALERT_DAYS = int(os.environ.get("ALERT_DAYS", "30"))

app = Flask(__name__)
app.secret_key = APP_SECRET

app.config.update(
    SESSION_COOKIE_HTTPONLY=True,
    SESSION_COOKIE_SAMESITE="Lax",
)

# =========================
# OAuth Google
# =========================
oauth = OAuth(app)

google = oauth.register(
    name="google",
    client_id=GOOGLE_CLIENT_ID,
    client_secret=GOOGLE_CLIENT_SECRET,
    access_token_url="https://oauth2.googleapis.com/token",
    authorize_url="https://accounts.google.com/o/oauth2/v2/auth",
    api_base_url="https://www.googleapis.com/oauth2/v2/",
    client_kwargs={"scope": "email profile"},
)

# =========================
# DB helpers
# =========================
def get_db():
    """Obtiene conexión SQLite por request (guardada en flask.g)."""
    if "db" not in g:
        g.db = sqlite3.connect(DATABASE)
        g.db.row_factory = sqlite3.Row
        g.db.execute("PRAGMA foreign_keys = ON;")
    return g.db


@app.teardown_appcontext
def close_db(exception=None):
    db = g.pop("db", None)
    if db is not None:
        db.close()


def _col_exists(db, table: str, col: str) -> bool:
    rows = db.execute(f"PRAGMA table_info({table})").fetchall()
    return any(r["name"] == col for r in rows)


def _add_col_if_missing(db, table: str, col: str, coltype: str):
    if not _col_exists(db, table, col):
        db.execute(f"ALTER TABLE {table} ADD COLUMN {col} {coltype};")


def init_db():
    """Crea tablas/columnas si faltan (idempotente)."""
    db = get_db()

    db.execute("""
        CREATE TABLE IF NOT EXISTS clients (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            full_name TEXT NOT NULL,
            dni TEXT NOT NULL,
            birth_date TEXT,
            phone TEXT,
            address TEXT,
            email TEXT,
            current_operator TEXT,
            current_tariff_price TEXT,
            permanence TEXT,
            terminal TEXT,
            sales_done TEXT,
            repairs_done TEXT,
            procedures_done TEXT,
            observations TEXT,
            pending_tasks TEXT,
            created_at TEXT NOT NULL
        );
    """)

    # Campos viejos (compatibilidad)
    _add_col_if_missing(db, "clients", "permanence_start", "TEXT")
    _add_col_if_missing(db, "clients", "permanence_end", "TEXT")

    # Campos nuevos (los que usa tu client_form.html)
    _add_col_if_missing(db, "clients", "permanence_start_date", "TEXT")
    _add_col_if_missing(db, "clients", "permanence_months", "INTEGER")
    _add_col_if_missing(db, "clients", "permanence_end_date", "TEXT")

    db.execute("""
        CREATE TABLE IF NOT EXISTS mobile_lines (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            client_id INTEGER NOT NULL,
            line_number TEXT,
            pin TEXT,
            puk TEXT,
            icc TEXT,
            google_or_iphone_account TEXT,
            created_at TEXT NOT NULL,
            FOREIGN KEY (client_id) REFERENCES clients(id) ON DELETE CASCADE
        );
    """)

    # ✅ ESTA TABLA FALTABA EN TU CÓDIGO PEGADO
    db.execute("""
        CREATE TABLE IF NOT EXISTS repairs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            client_id INTEGER NOT NULL,
            date TEXT,
            model TEXT,
            repair TEXT,
            cost REAL,
            created_at TEXT NOT NULL,
            FOREIGN KEY (client_id) REFERENCES clients(id) ON DELETE CASCADE
        );
    """)

    db.execute("""
        CREATE TABLE IF NOT EXISTS sales (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            client_id INTEGER NOT NULL,
            date TEXT,
            item TEXT,
            operator TEXT,
            amount REAL,
            notes TEXT,
            created_at TEXT NOT NULL,
            FOREIGN KEY (client_id) REFERENCES clients(id) ON DELETE CASCADE
        );
    """)

    # --- Backfill: sincronizar columnas de fin de permanencia ---
    db.execute("""
        UPDATE clients
        SET permanence_end_date = permanence_end
        WHERE (permanence_end_date IS NULL OR permanence_end_date = '')
          AND permanence_end IS NOT NULL AND permanence_end != '';
    """)

    db.execute("""
        UPDATE clients
        SET permanence_end = permanence_end_date
        WHERE (permanence_end IS NULL OR permanence_end = '')
          AND permanence_end_date IS NOT NULL AND permanence_end_date != '';
    """)

    db.commit()


# Inicializamos DB una vez al arrancar
with app.app_context():
    init_db()

# =========================
# Utilidades fechas
# =========================
def parse_yyyy_mm_dd(s: str):
    s = (s or "").strip()
    if not s:
        return None

    # Acepta varios formatos de fecha
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y"):
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            pass

    return None


def add_months(d: date, months: int) -> date:
    y = d.year + (d.month - 1 + months) // 12
    m = (d.month - 1 + months) % 12 + 1
    day = d.day

    if m == 12:
        next_month = date(y + 1, 1, 1)
    else:
        next_month = date(y, m + 1, 1)

    last_day = (next_month - timedelta(days=1)).day
    if day > last_day:
        day = last_day
    return date(y, m, day)


def compute_permanence_end(start_str: str, months_str: str, end_str: str):
    """
    Devuelve (start_iso, months_int, end_iso)
    - Si end_str viene informado, se respeta.
    - Si no, y hay start+months, se calcula.
    """
    start = parse_yyyy_mm_dd(start_str)
    end = parse_yyyy_mm_dd(end_str)

    months_int = None
    ms = (months_str or "").strip()
    if ms:
        try:
            months_int = int(ms)
        except ValueError:
            months_int = None

    if end is None and start is not None and months_int is not None:
        end = add_months(start, months_int)

    start_iso = start.isoformat() if start else None
    end_iso = end.isoformat() if end else None
    return start_iso, months_int, end_iso


def get_end_date_from_client_row(c):
    """Devuelve la fecha fin de permanencia en ISO o None. sqlite3.Row NO tiene .get()."""
    if not c:
        return None

    keys = c.keys()
    end_iso = None

    # preferimos la nueva columna
    if "permanence_end_date" in keys:
        end_iso = c["permanence_end_date"]

    # fallback a la columna vieja
    if (not end_iso) and ("permanence_end" in keys):
        end_iso = c["permanence_end"]

    return (end_iso or "").strip() or None


def days_until(end_iso: str):
    d = parse_yyyy_mm_dd(end_iso)
    if not d:
        return None
    return (d - date.today()).days

# =========================
# Auth
# =========================
def login_required(view):
    @wraps(view)
    def wrapped(*args, **kwargs):
        if not session.get("user"):
            return redirect(url_for("login"))
        return view(*args, **kwargs)
    return wrapped


def is_allowed(email):
    if not ALLOWED_EMAILS:
        return True
    return (email or "").lower() in ALLOWED_EMAILS

# =========================
# Routes
# =========================
@app.route("/")
def home():
    if session.get("user"):
        return redirect(url_for("clients"))
    return redirect(url_for("login"))


@app.route("/login")
def login():
    return render_template("login.html")


@app.route("/auth/google")
def auth_google():
    remember = request.args.get("remember") == "1"
    session["remember_me"] = remember
    redirect_uri = url_for("auth_google_callback", _external=True)
    return google.authorize_redirect(redirect_uri)


@app.route("/auth/google/callback")
def auth_google_callback():
    google.authorize_access_token()
    userinfo = google.get("userinfo").json()

    email = userinfo.get("email")
    if not is_allowed(email):
        session.clear()
        flash("Cuenta no autorizada", "danger")
        return redirect(url_for("login"))

    session["user"] = {"email": email, "name": userinfo.get("name") or email}

    if session.get("remember_me"):
        session.permanent = True
        app.permanent_session_lifetime = timedelta(days=30)

    session.pop("remember_me", None)
    return redirect(url_for("clients"))


@app.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("login"))


@app.route("/clients")
@login_required
def clients():
    db = get_db()
    q = request.args.get("q", "").strip()

    if q:
        rows = db.execute(
            """
            SELECT * FROM clients
            WHERE full_name LIKE ? OR dni LIKE ? OR phone LIKE ?
            ORDER BY id DESC
            """,
            (f"%{q}%", f"%{q}%", f"%{q}%")
        ).fetchall()
    else:
        rows = db.execute("SELECT * FROM clients ORDER BY id DESC").fetchall()

    days_left_map = {}
    for c in rows:
        end_iso = get_end_date_from_client_row(c)
        days_left_map[c["id"]] = days_until(end_iso) if end_iso else None

    return render_template(
        "clients_list.html",
        clients=rows,
        q=q,
        alert_days=ALERT_DAYS,
        days_left_map=days_left_map
    )


@app.route("/calendar", endpoint="calendar_view")
@login_required
def calendar_view():
    days = request.args.get("days", "365").strip()
    try:
        days_int = int(days)
    except ValueError:
        days_int = 365

    today = date.today()
    limit = today + timedelta(days=days_int)

    db = get_db()
    rows = db.execute(
        """
        SELECT id, full_name, phone, dni,
               COALESCE(NULLIF(permanence_end_date,''), permanence_end) AS end_date
        FROM clients
        WHERE (permanence_end_date IS NOT NULL AND permanence_end_date != '')
           OR (permanence_end IS NOT NULL AND permanence_end != '')
        ORDER BY end_date ASC
        """
    ).fetchall()

    upcoming = []
    for r in rows:
        end_d = parse_yyyy_mm_dd(r["end_date"])
        if not end_d:
            continue

        # mostramos solo dentro del rango (hoy -> hoy + days)
        if today <= end_d <= limit:
            upcoming.append((r, (end_d - today).days))

    return render_template(
        "calendar.html",
        upcoming=upcoming,
        days=days_int,
        alert_days=ALERT_DAYS
    )


@app.route("/api/permanencias", endpoint="api_permanencias")
@login_required
def api_permanencias():
    db = get_db()
    rows = db.execute(
        """
        SELECT id, full_name, phone, email, current_operator,
               COALESCE(NULLIF(permanence_end_date,''), permanence_end) AS end_date
        FROM clients
        WHERE (permanence_end_date IS NOT NULL AND permanence_end_date != '')
           OR (permanence_end IS NOT NULL AND permanence_end != '')
        ORDER BY end_date ASC
        """
    ).fetchall()

    out = []
    for r in rows:
        out.append({
            "id": r["id"],
            "full_name": r["full_name"],
            "phone": r["phone"],
            "email": r["email"],
            "current_operator": r["current_operator"],
            "permanence_end_date": r["end_date"],
            "days_left": days_until(r["end_date"]) if r["end_date"] else None,
            "url": url_for("view_client", client_id=r["id"])
        })
    return jsonify(out)


@app.route("/clients/new", methods=["GET", "POST"])
@login_required
def new_client():
    if request.method == "POST":
        db = get_db()

        p_start, p_months, p_end = compute_permanence_end(
            request.form.get("permanence_start_date") or request.form.get("permanence_start"),
            request.form.get("permanence_months"),
            request.form.get("permanence_end_date") or request.form.get("permanence_end"),
        )

        cur = db.execute("""
            INSERT INTO clients (
                full_name, dni, birth_date, phone, address, email,
                current_operator, current_tariff_price,
                permanence,
                permanence_start, permanence_end,
                permanence_start_date, permanence_months, permanence_end_date,
                terminal, sales_done, repairs_done, procedures_done, observations,
                pending_tasks, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            request.form["full_name"],
            request.form["dni"],
            request.form.get("birth_date"),
            request.form.get("phone"),
            request.form.get("address"),
            request.form.get("email"),
            request.form.get("current_operator"),
            request.form.get("current_tariff_price"),
            request.form.get("permanence"),
            p_start, p_end,
            p_start, p_months, p_end,
            request.form.get("terminal"),
            request.form.get("sales_done"),
            request.form.get("repairs_done"),
            request.form.get("procedures_done"),
            request.form.get("observations"),
            request.form.get("pending_tasks"),
            datetime.utcnow().isoformat()
        ))

        client_id = cur.lastrowid
        db.commit()
        return redirect(url_for("view_client", client_id=client_id))

    return render_template("client_form.html", client=None, lines=[], repairs=[], sales=[], alert_days=ALERT_DAYS)


@app.route("/clients/<int:client_id>")
@login_required
def view_client(client_id):
    db = get_db()

    client = db.execute("SELECT * FROM clients WHERE id = ?", (client_id,)).fetchone()
    if client is None:
        flash("Cliente no encontrado", "danger")
        return redirect(url_for("clients"))

    lines = db.execute(
        "SELECT * FROM mobile_lines WHERE client_id = ? ORDER BY id DESC",
        (client_id,)
    ).fetchall()

    repairs = db.execute(
        "SELECT * FROM repairs WHERE client_id = ? ORDER BY id DESC",
        (client_id,)
    ).fetchall()

    sales = db.execute(
        "SELECT * FROM sales WHERE client_id = ? ORDER BY id DESC",
        (client_id,)
    ).fetchall()

    end_iso = get_end_date_from_client_row(client)
    du = days_until(end_iso) if end_iso else None

    return render_template(
        "client_form.html",
        client=client,
        lines=lines,
        repairs=repairs,
        sales=sales,
        alert_days=ALERT_DAYS,
        days_left=du
    )


@app.route("/clients/<int:client_id>/update", methods=["POST"])
@login_required
def update_client(client_id):
    db = get_db()

    p_start, p_months, p_end = compute_permanence_end(
        request.form.get("permanence_start_date") or request.form.get("permanence_start"),
        request.form.get("permanence_months"),
        request.form.get("permanence_end_date") or request.form.get("permanence_end"),
    )

    db.execute("""
        UPDATE clients SET
            full_name = ?,
            dni = ?,
            birth_date = ?,
            phone = ?,
            address = ?,
            email = ?,
            current_operator = ?,
            current_tariff_price = ?,
            permanence = ?,

            permanence_start = ?,
            permanence_end = ?,

            permanence_start_date = ?,
            permanence_months = ?,
            permanence_end_date = ?,

            terminal = ?,
            sales_done = ?,
            repairs_done = ?,
            procedures_done = ?,
            observations = ?,
            pending_tasks = ?
        WHERE id = ?
    """, (
        request.form["full_name"],
        request.form["dni"],
        request.form.get("birth_date"),
        request.form.get("phone"),
        request.form.get("address"),
        request.form.get("email"),
        request.form.get("current_operator"),
        request.form.get("current_tariff_price"),
        request.form.get("permanence"),

        p_start,
        p_end,

        p_start,
        p_months,
        p_end,

        request.form.get("terminal"),
        request.form.get("sales_done"),
        request.form.get("repairs_done"),
        request.form.get("procedures_done"),
        request.form.get("observations"),
        request.form.get("pending_tasks"),
        client_id
    ))

    # Guardar líneas móviles (borramos y reinsertamos)
    db.execute("DELETE FROM mobile_lines WHERE client_id = ?", (client_id,))

    line_count = int(request.form.get("line_count", "0") or "0")
    for i in range(line_count):
        line_number = (request.form.get(f"line_number_{i}") or "").strip()
        pin = (request.form.get(f"pin_{i}") or "").strip()
        puk = (request.form.get(f"puk_{i}") or "").strip()
        icc = (request.form.get(f"icc_{i}") or "").strip()
        account = (request.form.get(f"account_{i}") or "").strip()

        if not (line_number or pin or puk or icc or account):
            continue

        db.execute("""
            INSERT INTO mobile_lines (
                client_id, line_number, pin, puk, icc, google_or_iphone_account, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (
            client_id,
            line_number,
            pin,
            puk,
            icc,
            account,
            datetime.utcnow().isoformat()
        ))

    db.commit()
    flash("Cliente actualizado", "success")
    return redirect(url_for("view_client", client_id=client_id))


@app.route("/clients/<int:client_id>/repairs/add", methods=["POST"])
@login_required
def add_repair(client_id):
    db = get_db()

    date_ = request.form.get("repair_date")
    model = request.form.get("repair_model")
    repair = request.form.get("repair_text")

    cost_raw = (request.form.get("repair_cost") or "").strip()
    cost = None
    if cost_raw:
        try:
            cost = float(cost_raw.replace(",", "."))
        except ValueError:
            cost = None

    db.execute("""
        INSERT INTO repairs (client_id, date, model, repair, cost, created_at)
        VALUES (?, ?, ?, ?, ?, ?)
    """, (
        client_id,
        date_,
        model,
        repair,
        cost,
        datetime.utcnow().isoformat()
    ))
    db.commit()
    return redirect(url_for("view_client", client_id=client_id))


@app.route("/clients/<int:client_id>/repairs/<int:repair_id>/delete", methods=["POST"])
@login_required
def delete_repair(client_id, repair_id):
    db = get_db()
    db.execute("DELETE FROM repairs WHERE id = ? AND client_id = ?", (repair_id, client_id))
    db.commit()
    return redirect(url_for("view_client", client_id=client_id))


@app.route("/clients/<int:client_id>/sales/add", methods=["POST"])
@login_required
def add_sale(client_id):
    db = get_db()

    date_ = request.form.get("sale_date")
    item = request.form.get("sale_item")
    operator = request.form.get("sale_operator")

    amount_raw = (request.form.get("sale_amount") or "").strip()
    amount = None
    if amount_raw:
        try:
            amount = float(amount_raw.replace(",", "."))
        except ValueError:
            amount = None

    notes = request.form.get("sale_notes")

    db.execute("""
        INSERT INTO sales (client_id, date, item, operator, amount, notes, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, (
        client_id,
        date_,
        item,
        operator,
        amount,
        notes,
        datetime.utcnow().isoformat()
    ))
    db.commit()
    return redirect(url_for("view_client", client_id=client_id))


@app.route("/clients/<int:client_id>/sales/<int:sale_id>/delete", methods=["POST"])
@login_required
def delete_sale(client_id, sale_id):
    db = get_db()
    db.execute("DELETE FROM sales WHERE id = ? AND client_id = ?", (sale_id, client_id))
    db.commit()
    return redirect(url_for("view_client", client_id=client_id))


@app.route("/clients/<int:client_id>/delete", methods=["POST"])
@login_required
def delete_client(client_id):
    db = get_db()
    db.execute("DELETE FROM clients WHERE id = ?", (client_id,))
    db.commit()
    flash("Cliente eliminado", "success")
    return redirect(url_for("clients"))


if __name__ == "__main__":
    app.run(debug=True)
