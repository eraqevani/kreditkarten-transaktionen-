import sqlite3
import pandas as pd

# Verbindung
conn = sqlite3.connect("../DWH.db")

# --- 1. Betrugsfälle pro Monat/Jahr ---
sql1 = """SELECT z.jahr, z.monat, SUM(f.betrug) AS anzahl_betrugsfaelle
FROM fact_transactions f
JOIN dim_zeit z ON f.zeit_id = z.zeit_id
GROUP BY z.jahr, z.monat
ORDER BY z.jahr, z.monat"""
df_betrug_monat = pd.read_sql_query(sql1, conn)
print("Betrugsfälle pro Monat/Jahr:")
print(df_betrug_monat)

# --- 2. Umsatz pro Kunde ---
sql2 = """SELECT k.karten_id, k.kartennummer, k.vorname, k.nachname, SUM(f.betrag) AS umsatz
FROM fact_transactions f
JOIN dim_karten k ON f.karten_id = k.karten_id
GROUP BY k.karten_id, k.kartennummer
ORDER BY umsatz DESC
LIMIT 10
"""
df_umsatz_kunde = pd.read_sql_query(sql2, conn)
print("------------------------------------------------------------------------")
print("\nTop Kunden nach Umsatz:")
print(df_umsatz_kunde.head(10))


# --- 3. Betrugsquote pro Kunde ---
sql3 = """
SELECT k.karten_id, k.kartennummer, k.vorname, k.nachname,
       SUM(f.betrug)*1.0 / COUNT(f.transaktionsnummer) AS betrugsquote
FROM fact_transactions f
JOIN dim_karten k ON f.karten_id = k.karten_id
GROUP BY k.karten_id, k.kartennummer, k.vorname, k.nachname
ORDER BY betrugsquote DESC
LIMIT 20
"""
df_betrugsquote_kunde = pd.read_sql_query(sql3, conn)
print("\nTop Kunden nach Betrugsquote:")
print(df_betrugsquote_kunde)




# --- 4. Top-Händler nach Umsatz ---
sql4 = """SELECT h.haendler_id, h.name, SUM(f.betrag) AS umsatz
FROM fact_transactions f
JOIN dim_haendler h ON f.haendler_id = h.haendler_id
GROUP BY h.haendler_id, h.name
ORDER BY umsatz DESC
"""
df_top_haendler = pd.read_sql_query(sql4, conn)
print("---------------------------------------------------------------------------------------")
print("\nTop-Händler nach Umsatz:")
print(df_top_haendler.head(10))

# --- 5. Betrugsquote pro Händler ---
sql5 = """SELECT h.haendler_id, h.name,
       SUM(f.betrug)*1.0 / COUNT(f.transaktionsnummer) AS betrugsquote
FROM fact_transactions f
JOIN dim_haendler h ON f.haendler_id = h.haendler_id
GROUP BY h.haendler_id, h.name
ORDER BY betrugsquote DESC
"""
df_betrugsquote = pd.read_sql_query(sql5, conn)
print("-------------------------------------------------------------------------------------------------")
print("\nBetrugsquote pro Händler:")
print(df_betrugsquote.head(10))








#6. Umsatz pro Kunde in GBP
sql6= """SELECT k.karten_id, k.kartennummer, k.vorname, k.nachname,
       SUM(f.betrag / w.wechselkurs) AS umsatz_gbp,
       AVG(w.wechselkurs) AS durchschnittskurs
FROM fact_transactions f
JOIN dim_karten k ON f.karten_id = k.karten_id
JOIN dim_zeit z ON f.zeit_id = z.zeit_id
JOIN dim_wechselkurs w 
  ON w.datum = z.datum
  AND w.waehrung_code = 'GBP'
GROUP BY k.karten_id, k.kartennummer, k.vorname, k.nachname
ORDER BY umsatz_gbp DESC
LIMIT 10
"""
df_umsatz_gbp = pd.read_sql_query(sql6, conn)
print("\nTop Kunden nach Umsatz in GBP:")
print(df_umsatz_gbp)




# 7.Umsatz pro Monat pro Kunde für das letzte Jahr (EUR)
sql7= """SELECT k.karten_id,
       k.kartennummer,
       z.monat,
       z.jahr,
       SUM(f.betrag) AS umsatz
FROM fact_transactions f
JOIN dim_karten k ON f.karten_id = k.karten_id
JOIN dim_zeit z ON f.zeit_id = z.zeit_id
LEFT JOIN dim_wechselkurs w 
       ON f.wechselkurs_id = w.wechselkurs_id
      AND w.waehrung_code = 'EUR'
WHERE z.jahr = (
    SELECT MAX(z1.jahr)
    FROM fact_transactions f1
    JOIN dim_zeit z1 ON f1.zeit_id = z1.zeit_id)
GROUP BY k.karten_id, k.kartennummer, z.monat, z.jahr
ORDER BY k.karten_id, z.monat
LIMIT 15"""


df_umsatz_monat_kunde = pd.read_sql_query(sql7, conn)

print("Umsatz pro Kunde pro Monat im letzten Jahr (EUR):")
print(df_umsatz_monat_kunde)




# Verbindung schließen
conn.close()