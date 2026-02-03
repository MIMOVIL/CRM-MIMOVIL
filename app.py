import os
import sqlite3
import calendar
from datetime import datetime, timedelta
from functools import wraps

from flask import Flask, g, redirect, render_template, request, session, url_for, flash, jsonify
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

app = Flask(__name__)
app.secret_key = APP_SECRET

app.config.update(
    SESSION_COOKIE_HTTPONLY=True,
    SESSION_COOKIE_SAMESITE="Lax",
)

# =========================
# OAuth Google (SIN OpenID / SIN nonce)
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
# Base de datos
# =========================
def get_db():
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


def ensure_column(table, column, coltype):
    """
    Añade columna si no existe (para no romper la BD actual)
    """
    db = get_db()
    cols = [r["name"] for r in db.execute(f"PRAGMA table_info({table})").fetchall()]
    if column not in cols:
        db.execute(f"ALTER TABLE {table} ADD COLUMN {column} {coltype};")
        db.commit()


def calc_end_date(start_date_str, months):
    """
    start_date_str: 'YYYY-MM-DD'
    months: int o str
    devuelve 'YYYY-MM-DD' o '' si no se puede calcular
    """
    if not start_date_str:
        return ""

    try:
        m = int(months or 0)
    except ValueError:
        m = 0

    if m <= 0:
        return ""

    try:
        d = datetime.strptime(start_date_str, "%Y-%m-%d").date()
    except ValueError:
        return ""

    y = d.year + (d.month - 1 + m) // 12
    mm = (d.month - 1 + m) % 12 + 1
    day = d.day

    last_day = calendar.monthrange(y, mm)[1]
    if day > last_day:
        day = last_day

    end = d.replace(year=y, month=mm, day=day)
    return end.strftime("%Y-%m-%d")


def init_db():
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

    db.commit()

    # ✅ NUEVO: columnas para permanencia + calendario (sin romper tu BD)
    ensure_column("clients", "permanence_start_date", "TEXT")
    ensure_column("clients", "permanence_months", "INTEGER")
    ensure_column("clients", "permanence_end_date", "TEXT")


@app.before_request
def ensure_db():
    init_db()

# =========================
# Autenticación
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
# Rutas
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
    q = request.args.get("q", "")
    if q:
        rows = db.execute(
            "SELECT * FROM clients WHERE full_name LIKE ? OR dni LIKE ? OR phone LIKE ? ORDER BY id DESC",
            (f"%{q}%", f"%{q}%", f"%{q}%")
        ).fetchall()
    else:
        rows = db.execute("SELECT * FROM clients ORDER BY id DESC").fetchall()
    return render_template("clients_list.html", clients=rows, q=q)


@app.route("/clients/new", methods=["GET", "POST"])
@login_required
def new_client():
    if request.method == "POST":
        db = get_db()

        perm_start = request.form.get("permanence_start_date")
        perm_months = request.form.get("permanence_months")
        perm_end = calc_end_date(perm_start, perm_months)

        cur = db.execute("""
            INSERT INTO clients (
                full_name, dni, birth_date, phone, address, email,
                current_operator, current_tariff_price, permanence, terminal,
                sales_done, repairs_done, procedures_done, observations,
                pending_tasks,
                permanence_start_date, permanence_months, permanence_end_date,
                created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
            request.form.get("terminal"),
            request.form.get("sales_done"),
            request.form.get("repairs_done"),
            request.form.get("procedures_done"),
            request.form.get("observations"),
            request.form.get("pending_tasks"),
            perm_start,
            perm_months if perm_months not in ("", None) else None,
            perm_end,
            datetime.utcnow().isoformat()
        ))

        client_id = cur.lastrowid
        db.commit()
        return redirect(url_for("view_client", client_id=client_id))

    return render_template("client_form.html", client=None, lines=[], repairs=[], sales=[])


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

    return render_template("client_form.html", client=client, lines=lines, repairs=repairs, sales=sales)


@app.route("/clients/<int:client_id>/update", methods=["POST"])
@login_required
def update_client(client_id):
    db = get_db()

    perm_start = request.form.get("permanence_start_date")
    perm_months = request.form.get("permanence_months")
    perm_end = calc_end_date(perm_start, perm_months)

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
            terminal = ?,
            sales_done = ?,
            repairs_done = ?,
            procedures_done = ?,
            observations = ?,
            pending_tasks = ?,
            permanence_start_date = ?,
            permanence_months = ?,
            permanence_end_date = ?
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
        request.form.get("terminal"),
        request.form.get("sales_done"),
        request.form.get("repairs_done"),
        request.form.get("procedures_done"),
        request.form.get("observations"),
        request.form.get("pending_tasks"),
        perm_start,
        perm_months if perm_months not in ("", None) else None,
        perm_end,
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


# =========================
# Reparaciones
# =========================
@app.route("/clients/<int:client_id>/repairs/add", methods=["POST"])
@login_required
def add_repair(client_id):
    db = get_db()

    date = request.form.get("repair_date")
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
        date,
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


# =========================
# Ventas
# =========================
@app.route("/clients/<int:client_id>/sales/add", methods=["POST"])
@login_required
def add_sale(client_id):
    db = get_db()

    date = request.form.get("sale_date")
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
        date,
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


# =========================
# Calendario permanencias
# =========================
@app.route("/calendar")
@login_required
def calendar_view():
    return render_template("calendar.html")


@app.route("/api/permanencias")
@login_required
def api_permanencias():
    db = get_db()
    rows = db.execute("""
        SELECT id, full_name, phone, permanence_end_date
        FROM clients
        WHERE permanence_end_date IS NOT NULL AND permanence_end_date != ''
        ORDER BY permanence_end_date ASC
    """).fetchall()

    events = []
    for r in rows:
        events.append({
            "title": f"{r['full_name']} (fin permanencia)",
            "start": r["permanence_end_date"],
            "url": url_for("view_client", client_id=r["id"]),
        })
    return jsonify(events)


# =========================
# Borrar cliente
# =========================
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
