    # Guardar líneas móviles (borramos y reinsertamos)
    db.execute("DELETE FROM mobile_lines WHERE client_id = ?", (client_id,))

    line_count = int(request.form.get("line_count", "0") or "0")

    for i in range(line_count):
        line_number = (request.form.get(f"line_number_{i}") or "").strip()
        pin = (request.form.get(f"pin_{i}") or "").strip()
        puk = (request.form.get(f"puk_{i}") or "").strip()
        icc = (request.form.get(f"icc_{i}") or "").strip()
        account = (request.form.get(f"account_{i}") or "").strip()

        # ✅ nuevo: fin permanencia por línea
        line_perm_end = (request.form.get(f"line_perm_end_{i}") or "").strip()

        # ✅ MUY IMPORTANTE:
        # antes ignorabas la línea si no había número/pin/puk/icc/account
        # pero ahora puede que solo rellenes la fecha. Así que la incluimos en la condición.
        if not (line_number or pin or puk or icc or account or line_perm_end):
            continue

        db.execute("""
            INSERT INTO mobile_lines (
                client_id, line_number, pin, puk, icc,
                google_or_iphone_account, permanence_end_date, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            client_id,
            line_number,
            pin,
            puk,
            icc,
            account,
            line_perm_end,
            datetime.utcnow().isoformat()
        ))
