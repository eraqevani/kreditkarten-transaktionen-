import sqlite3


def run_etl_creditcard_to_staging():
    """ETL: Daten aus CreditCard.db in Staging.db laden"""

    # Verbindungen
    staging_conn = sqlite3.connect("../Staging.db")
    source_conn = sqlite3.connect("../CreditCard.db")

    cursor = staging_conn.cursor()
    # Attach CreditCard.db für direkte SQL-Zugriffe
    cursor.execute("ATTACH DATABASE '../CreditCard.db' AS source")


    # KUNDEN

    cursor.execute("""
        INSERT OR IGNORE INTO stg_kunden (vorname, nachname, geschlecht, geburtsdatum, beruf)
        SELECT DISTINCT first, last, gender, dob, job
        FROM transactions
    """)
    staging_conn.commit()
    print(f"Kunden geladen")


    # HÄNDLER

    cursor.execute("""
        INSERT OR IGNORE INTO stg_haendler (haendlername, kategorie)
        SELECT DISTINCT merchant, category
        FROM transactions
    """)
    staging_conn.commit()
    print(f"Händler geladen")


    # ADRESSEN

    # alle gültigen Städte aus source.cities
    cursor.execute("SELECT name, subcountry FROM cities")
    valid_cities = set(cursor.fetchall())
    valid_city_names = set(name for name, subcountry in valid_cities)

    # Adressen aus transactions
    cursor.execute("SELECT city, state, merch_zipcode FROM transactions")
    rows = cursor.fetchall()

    valid_rows = [(city, state, zips) for city, state, zips in rows if city in valid_city_names]
    invalid_rows = [(city, state, zips, "Ungültige Stadt") for city, state, zips in rows if city not in valid_city_names]

    if valid_rows:
        cursor.executemany("""
            INSERT OR IGNORE INTO stg_adressen (stadt, region, plz)
            VALUES (?, ?, ?)
        """, valid_rows)

    if invalid_rows:
        cursor.executemany("""
            INSERT OR IGNORE INTO stg_ungueltige_adressen (stadt, region, plz, grund)
            VALUES (?, ?, ?, ?)
        """, invalid_rows)

    staging_conn.commit()
    print(f"{len(valid_rows)} gültige Adressen, {len(invalid_rows)} ungültige Adressen geladen")


    # KARTEN

    # Karten direkt aus transactions + stg_kunden JOIN
    cursor.execute("""
        INSERT OR IGNORE INTO stg_karten (kartennummer, kunden_id)
        SELECT DISTINCT t.cc_num, k.kunden_id
        FROM source.transactions t
        JOIN stg_kunden k
            ON t.first = k.vorname
            AND t.last =k.nachname
        WHERE t.cc_num IS NOT NULL
    """)
    staging_conn.commit()
    print("Karten geladen und Kunden zugeordnet")


    # TRANSNAKTIONEN

    cursor.execute("""
        INSERT OR IGNORE INTO stg_transaktionen
        (transaktionsnummer, transaktionszeit, betrag, 
        betrug, karten_id, haendler_id, adresse_id)
        SELECT 
            t.trans_num,
            t.trans_date_trans_time,
            t.amt,
            t.is_fraud,
            k.karten_id,
            h.handler_id,
            a.adresse_id
        FROM source.transactions t
        LEFT JOIN stg_karten k
            ON t.cc_num IS NOT NULL
            AND k.kartennummer = t.cc_num
        LEFT JOIN stg_haendler h
            ON h.haendlername = t.merchant
            AND h.kategorie = t.category
        LEFT JOIN stg_adressen a
            ON a.stadt = t.city
            AND a.region = t.state
            AND a.plz = t.merch_zipcode
    """)
    staging_conn.commit()
    print("Transaktionen geladen")


    # WECHSELKURSE

    # Explodiere EUR + andere Währungen
    cursor.execute("SELECT Date, USD, CHF, "
                   "GBP FROM exchange_rates")
    rows = cursor.fetchall()

    insert_rows = []
    for datum, usd, chf, gbp in rows:
        daten = [("EUR", 1.0), ("USD", usd),
                 ("CHF", chf), ("GBP", gbp)]
        for waehrung_code, wechselkurs in daten:
            insert_rows.append((datum, waehrung_code,
                                wechselkurs))

    if insert_rows:
        cursor.executemany("""
            INSERT OR IGNORE INTO stg_wechselkurse 
            (datum, waehrung_code, wechselkurs)
            VALUES (?, ?, ?)
        """, insert_rows)
        staging_conn.commit()
    print(" Wechselkurs-Zeilen geladen")


    # Verbindungen schließen

    cursor.execute("DETACH DATABASE source")
    staging_conn.close()
    source_conn.close()
    print("ETL-Prozess erfolgreich abgeschlossen")
    print("Daten erfolgreich eingefügt in Staging.db")


# Pipeline ausführen
run_etl_creditcard_to_staging()


