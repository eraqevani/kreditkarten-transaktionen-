import sqlite3

# Verbindung zur Staging DB
staging_conn = sqlite3.connect("../Staging.db")
cursor = staging_conn.cursor()

# -----------------------------
# Tabellen erstellen
# -----------------------------
cursor.execute("""CREATE TABLE IF NOT EXISTS stg_kunden 
    (kunden_id INTEGER PRIMARY KEY AUTOINCREMENT,
    vorname TEXT NOT NULL,
    nachname TEXT NOT NULL,
    geschlecht TEXT NOT NULL,
    geburtsdatum TEXT NOT NULL,
    beruf TEXT,
    UNIQUE(vorname, nachname, geburtsdatum))""")

cursor.execute("""CREATE TABLE IF NOT EXISTS stg_karten 
    (karten_id INTEGER PRIMARY KEY AUTOINCREMENT,
    kartennummer TEXT UNIQUE NOT NULL,
    kunden_id INTEGER NOT NULL,
    FOREIGN KEY(kunden_id) REFERENCES stg_kunden(kunden_id)) """)

cursor.execute("""CREATE TABLE IF NOT EXISTS stg_haendler 
    (handler_id INTEGER PRIMARY KEY AUTOINCREMENT,
    haendlername TEXT,
    kategorie TEXT,
    UNIQUE(haendlername, kategorie))""")

cursor.execute("""CREATE TABLE IF NOT EXISTS stg_adressen 
    (adresse_id INTEGER PRIMARY KEY AUTOINCREMENT,
    stadt TEXT NOT NULL,
    region TEXT NOT NULL,
    plz TEXT NOT NULL,
    UNIQUE(stadt, region, plz))""")

cursor.execute("""CREATE TABLE IF NOT EXISTS stg_ungueltige_adressen 
    (stadt TEXT NOT NULL,
    region TEXT NOT NULL,
    plz TEXT NOT NULL,
    grund TEXT NOT NULL)""")

cursor.execute("""CREATE TABLE IF NOT EXISTS stg_transaktionen 
    (transaktions_id INTEGER PRIMARY KEY AUTOINCREMENT,
    transaktionsnummer TEXT NOT NULL UNIQUE,
    transaktionszeit DATETIME NOT NULL,
    betrag REAL NOT NULL,
    betrug INTEGER DEFAULT 0,
    karten_id INTEGER NOT NULL, 
    haendler_id INTEGER NOT NULL,
    adresse_id INTEGER,
    FOREIGN KEY (karten_id) REFERENCES stg_karten(karten_id),
    FOREIGN KEY (haendler_id) REFERENCES stg_haendler(handler_id),
    FOREIGN KEY (adresse_id) REFERENCES stg_adressen(adresse_id))""")

cursor.execute("""CREATE TABLE IF NOT EXISTS stg_wechselkurse 
    (wechselkurs_id INTEGER PRIMARY KEY AUTOINCREMENT,
    datum DATE NOT NULL,
    waehrung_code TEXT NOT NULL,
    wechselkurs REAL NOT NULL,
    UNIQUE(datum, waehrung_code))""")

staging_conn.commit()

# Verbindung schließen

staging_conn.close()

print("Staging DB erstellt")
