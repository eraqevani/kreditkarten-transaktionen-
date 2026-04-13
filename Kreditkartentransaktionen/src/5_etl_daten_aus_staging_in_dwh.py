import sqlite3


def run_etl_staging_to_dwh():
    """ETL-Prozess: Daten aus Staging DB holen und in bestehende DWH-Tabellen laden."""

    staging_conn = sqlite3.connect("../Staging.db")
    dwh_conn = sqlite3.connect("../DWH.db")
    dwh_cursor = dwh_conn.cursor()
    # Attach Staging.db für direkte SQL-Zugriffe
    dwh_cursor.execute("ATTACH DATABASE '../Staging.db' AS staging")

    # DIMENSIONEN

    # Karten
    dwh_cursor.execute("""
        INSERT OR IGNORE INTO dim_karten (karten_id, kartennummer)
        SELECT karten_id, kartennummer
        FROM stg_karten
    """)

    # Händler
    dwh_cursor.execute("""
        INSERT OR IGNORE INTO dim_haendler (haendler_id, name, kategorie)
        SELECT handler_id, haendlername, kategorie
        FROM stg_haendler
    """)

    # Zeit
    dwh_cursor.execute("""
        INSERT OR IGNORE INTO dim_zeit (datum, jahr, monat, tag)
        SELECT DISTINCT
            DATE(transaktionszeit),
            CAST(strftime('%Y', transaktionszeit) AS INTEGER),
            CAST(strftime('%m', transaktionszeit) AS INTEGER),
            CAST(strftime('%d', transaktionszeit) AS INTEGER)
        FROM stg_transaktionen
    """)

    # Wechselkurse
    dwh_cursor.execute("""
        INSERT OR IGNORE INTO dim_wechselkurs 
        (wechselkurs_id,datum, waehrung_code, wechselkurs)
        SELECT wechselkurs_id,datum, waehrung_code, wechselkurs
        FROM stg_wechselkurse
    """)



    dwh_conn.commit()
    print("Dimensionen erfolgreich geladen")


    # FAKTENTABELLE

    dwh_cursor.execute("""
    INSERT OR REPLACE INTO fact_transactions
    (transaktionsnummer, betrag, betrug, karten_id, 
    haendler_id, zeit_id, wechselkurs_id)
    SELECT 
        t.transaktionsnummer,
        t.betrag,
        t.betrug,
        k.karten_id,
        h.haendler_id,
        z.zeit_id,
        w.wechselkurs_id
    FROM stg_transaktionen t
    LEFT JOIN dim_karten k
      ON k.karten_id = t.karten_id
    LEFT JOIN dim_haendler h
      ON h.haendler_id = t.haendler_id
    LEFT JOIN dim_zeit z
    
      ON DATE(t.transaktionszeit) = z.datum
    LEFT JOIN dim_wechselkurs w
      ON DATE(t.transaktionszeit) = w.datum
      AND w.waehrung_code = 'EUR'
    """)
    dwh_conn.commit()

    # Anzahl geladener Transaktionen
    count = dwh_cursor.execute("SELECT COUNT(*) FROM fact_transactions").fetchone()[0]
    print(f"{count} Transaktionen erfolgreich in Faktentabelle eingefügt")

    # Verbindung schließen
    dwh_cursor.execute("DETACH DATABASE staging")
    staging_conn.close()
    dwh_conn.close()

    print("ETL-Prozess abgeschlossen.")
    print("Daten erfolgreich eingefügt in DWH.db")

#Pipeline ausführens
run_etl_staging_to_dwh()