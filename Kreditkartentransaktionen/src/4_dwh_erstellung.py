import sqlite3

# Verbindung zur DWH-Datenbank herstellen (erstellt die DB, falls sie nicht existiert)
dwh_conn = sqlite3.connect("../DWH.db")
dwh_cursor = dwh_conn.cursor()

print("Data Warehouse DB erstellt / verbunden")

# Karten
dwh_cursor.execute("""CREATE TABLE IF NOT EXISTS dim_karten (
    karten_id INTEGER PRIMARY KEY,
    kartennummer TEXT)""")

# Händler
dwh_cursor.execute("""
CREATE TABLE IF NOT EXISTS dim_haendler (
    haendler_id INTEGER PRIMARY KEY,
    name TEXT,
    kategorie TEXT)""")


# Zeit
dwh_cursor.execute("""CREATE TABLE IF NOT EXISTS dim_zeit (
    zeit_id INTEGER PRIMARY KEY,
    datum TEXT,
    jahr INTEGER,
    monat INTEGER,
    tag INTEGER)""")


#Wechselkurs
dwh_cursor.execute("""CREATE TABLE IF NOT EXISTS dim_wechselkurs (
    wechselkurs_id INTEGER PRIMARY KEY,
    datum TEXT NOT NULL,
    waehrung_code TEXT NOT NULL,
    wechselkurs REAL NOT NULL,
    UNIQUE(datum, waehrung_code))""")
dwh_conn.commit()
print("Dimensionstabellen erstellt")




dwh_cursor.execute("""CREATE TABLE IF NOT EXISTS fact_transactions (
    id INTEGER PRIMARY KEY,
    transaktionsnummer TEXT UNIQUE,
    betrag REAL,
    betrug INTEGER,
    karten_id INTEGER,
    haendler_id INTEGER,
    zeit_id INTEGER,
    wechselkurs_id INTEGER,
    FOREIGN KEY(karten_id) REFERENCES dim_karten(karten_id),
    FOREIGN KEY(haendler_id) REFERENCES dim_haendler(haendler_id),
    FOREIGN KEY(zeit_id) REFERENCES dim_zeit(zeit_id),
    FOREIGN KEY (wechselkurs_id) REFERENCES dim_wechselkurs(wechselkurs_id))""")

dwh_conn.commit()
print("Faktentabelle erstellt")

dwh_conn.close()

