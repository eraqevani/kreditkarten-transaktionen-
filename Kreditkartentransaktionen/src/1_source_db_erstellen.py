import sqlite3
import pandas as pd
#Erstellung von Quelldatenbank CreditCard.db und laden
# die CSV-Dateien dort, ohne sie miteinander zu verbinden, so nur als Rohdaten
# Verbindung zur Datenbank
conn = sqlite3.connect("../CreditCard.db")
cursor = conn.cursor()

# CSV-Dateien und die Namen der Tabellen
csv_files = { "transactions": "../CSVfiles/credit_card_transactions.csv",
               "cities": "../CSVfiles/world-cities.csv",
               "exchange_rates": "../CSVfiles/exchangerate.csv"}

# Tabellen  aus CSV erstellen

for table_name, file_name in csv_files.items():
    df = pd.read_csv(file_name)
    df.to_sql(table_name, conn, if_exists="replace", index=False)
    print(f"Tabelle '{table_name}' erstellt mit {len(df)} Zeilen")

conn.close()
print("Source Database 'CreditCard.db' ist fertig!")