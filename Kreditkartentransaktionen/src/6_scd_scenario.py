import sqlite3

conn = sqlite3.connect("../DWH.db")
cursor = conn.cursor()

# Spalten hinzufügen, falls sie noch nicht existieren
try:
    cursor.execute("ALTER TABLE dim_karten ADD COLUMN kunden_id INTEGER")
except sqlite3.OperationalError:
    pass

try:
    cursor.execute("ALTER TABLE dim_karten ADD COLUMN vorname TEXT")
except sqlite3.OperationalError:
    pass

try:
    cursor.execute("ALTER TABLE dim_karten ADD COLUMN nachname TEXT")
except sqlite3.OperationalError:
    pass

conn.commit()


def update_dim_karten_scd1():
    """Aktualisiert vorhandene Karten mit Kundendaten (SCD1)"""
    conn = sqlite3.connect("../DWH.db")
    cursor = conn.cursor()

    #Staging.db anhängen
    cursor.execute("ATTACH DATABASE '../Staging.db' AS staging")

    # Karten + Kunden aus Staging
    cursor.execute("""SELECT ka.karten_id, ka.kartennummer, ku.kunden_id, ku.vorname, ku.nachname
        FROM stg_karten ka
        JOIN stg_kunden ku
          ON ka.kunden_id = ku.kunden_id""")
    rows = cursor.fetchall()

    for karten_id, kartennummer, kunden_id, vorname, nachname in rows:
        # Nur Update, da Karten schon existieren
        cursor.execute("""UPDATE dim_karten
            SET kunden_id = ?, vorname = ?, nachname = ?
            WHERE kartennummer = ? """, (kunden_id, vorname, nachname, kartennummer))

    conn.commit()
    conn.close()
    print(f"{len(rows)} vorhandene Karten mit Kundendaten aktualisiert (SCD1)")

update_dim_karten_scd1()

