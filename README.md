End‑to‑End Data Warehouse für Kreditkartentransaktionen
Erstellt von: Era Qevani

1. Einleitung
In diesem Projekt wurde ein vollständiges Data‑Warehouse‑System für Kreditkartentransaktionen entwickelt.
Der Fokus lag darauf, einen realistischen ETL‑Prozess aufzubauen, der Daten aus mehreren Quellen extrahiert, bereinigt, transformiert und in ein analytisches Star‑Schema überführt.
Das Projekt bildet einen kompletten Data‑Engineering‑Workflow ab:
- Datenintegration
- Datenbereinigung
- Staging‑Schicht
- Data‑Warehouse‑Modellierung
- SCD‑Implementierung
- Währungsumrechnung
- Erstellung analytischer KPIs
Die Daten stammen aus Kaggle und simulieren ein reales Kreditkartensystem mit Transaktionen, Händlern, Kunden, Städten und Wechselkursen.
2. Projektziele
Das Ziel des Projekts war es, ein professionelles Data‑Warehouse aufzubauen, das:
- große Datenmengen effizient verarbeitet
- Datenqualität sicherstellt
- ein sauberes Star‑Schema bereitstellt
- analytische Fragestellungen beantwortet
- Best Practices aus ETL und DWH berücksichtigt

3. Datenquellen (Kaggle)
Credit Card Transactions Dataset
Enthält über 1,2 Millionen Transaktionen mit Feldern wie:
- trans_num
- trans_date_trans_time
- amt
- is_fraud
- merchant
- category
- cc_num
- first / last
- city / state / zip
Annahme zur Währung:
Das Kaggle‑Dataset enthält keine explizite Währungsangabe.
Für dieses Projekt wurde angenommen, dass alle Beträge in EUR vorliegen.
World Cities Dataset
Wird zur Validierung 
Exchange Rates Dataset
Enthält Wechselkurse für USD, CHF, GBP.
EUR wurde als 1.0 ergänzt.

⚠️ Datenbereitstellung (CSV‑Dateien nicht im Repository)
Die Rohdaten sind größer als 100 MB und können daher nicht auf GitHub gespeichert werden.
Um das Projekt auszuführen:
1. Datensätze herunterladen
Lade die folgenden Dateien von Kaggle herunter:
- credit_card_transactions.csv
- exchangerate.csv
- world-cities.csv
2. Dateien lokal ablegen
Lege die Dateien in folgendem Ordner ab:
CSVfiles/


Die Struktur muss so aussehen:
Projektordner/
│
├── CSVfiles/
│   ├── credit_card_transactions.csv
│   ├── exchangerate.csv
│   └── world-cities.csv
│
├── src/
├── Dokumentation/
├── README.md
└── .gitignore

3. Projekt starten
python src/0_main.py



4. Architekturübersicht
Das System besteht aus drei Schichten:
4.1 Source Layer (CreditCard.db)
- CSV‑Dateien werden unverändert geladen
- Keine Transformationen
- Dient als „Single Source of Truth“
4.2 Staging Layer (Staging.db)
Hier findet die eigentliche Datenbereinigung statt:
- Deduplikation
- Validierung (Städte gegen world‑cities.csv)
- Transformationen
- Kunden aus Vor‑/Nachnamen
- Karten zu Kunden
- Händler kategorisiert
- Wechselkurse Column‑to‑Row
- Staging‑Transaktionen mit allen IDs
4.3 Data Warehouse Layer (DWH.db)
Star‑Schema mit:
Faktentabelle: fact_transactions
- transaktionsnummer
- betrag
- betrug
- karten_id
- haendler_id
- zeit_id
- wechselkurs_id
Dimensionen
- dim_karten (inkl. SCD1)
- dim_haendler
- dim_zeit
- dim_wechselkurs

5. ETL‑Prozess
5.1 Source → Staging
Kunden
- DISTINCT first, last, gender, dob, job
- Deduplikation
- Kunden‑ID erzeugt
Händler
- DISTINCT merchant, category
Adressen
- Validierung gegen world‑cities.csv
- Ungültige Adressen separat gespeichert
Karten
- Zuordnung zu Kunden
- DISTINCT cc_num + kunden_id
Transaktionen
- Joins mit Karten, Händlern, Adressen
- Bereinigung ungültiger Werte
Wechselkurse
- Column‑to‑Row
- EUR ergänzt
- Jede Zeile → 4 Datensätze

5.2 Staging → DWH
Dimensionen
- dim_karten
- dim_haendler
- dim_zeit
- dim_wechselkurs
SCD1 für dim_karten
- Vorname/Nachname werden aktualisiert
- Keine Historisierung
Faktentabelle
- Fremdschlüssel korrekt zugeordnet
- INSERT OR REPLACE für Konsistenz

6. Mapping‑Dokumentation
Für jede Tabelle wurde dokumentiert:
- Quelle
- Ziel
- Transformationstyp
- SQL‑Query
- Besonderheiten


7. OLAP‑Analysen
Implementierte Analysen:
- Betrugsfälle pro Monat
- Umsatz pro Kunde
- Betrugsquote pro Kunde
- Top‑Händler nach Umsatz
- Betrugsquote pro Händler
- Umsatz in GBP (Währungsumrechnung)
- Umsatz pro Kunde pro Monat im letzten Jahr

📎 Anhang
A1 – Star‑Schema des Data Warehouses
Das Star‑Schema bildet die zentrale Struktur des Data‑Warehouses ab.
In der Mitte befindet sich die Faktentabelle fact_transactions, die alle Transaktionen enthält.
Über Fremdschlüssel ist sie mit vier Dimensionstabellen verbunden:
- dim_karten – Informationen zu Karten und Kunden (inkl. SCD1‑Updates)
- dim_haendler – Händlername und Kategorie
- dim_zeit – Datum, Jahr, Monat, Tag
- dim_wechselkurs – Währungscode und Wechselkurs pro Datum
Diese Struktur ermöglicht effiziente OLAP‑Analysen wie Umsatz, Betrugsquote oder Händlerperformance.
![starschema_kreditkartentransaktionen.png](Dokumentation/starschema_kreditkartentransaktionen.png)


A2 – MER‑Modell (Entity‑Relationship‑Modell) des Data Warehouses
Das MER‑Modell zeigt die logischen Beziehungen zwischen den Tabellen im Data‑Warehouse.
Es stellt dar:
- welche Entitäten existieren (Karten, Händler, Zeit, Wechselkurse)
- welche Attribute sie besitzen
- wie sie über Primär‑ und Fremdschlüssel miteinander verbunden sind
Die Faktentabelle steht im Zentrum und verweist auf die Dimensionen, wodurch ein hierarchisches Analysemodell entsteht.
![mER_Kreditkartentransaktionen.png](Dokumentation/mER_Kreditkartentransaktionen.png)

A3 – ER‑Modell der Staging‑Datenbank
Das ER‑Modell der Staging‑Schicht bildet die Kernobjekte des Transaktionssystems ab.
Es zeigt:
- Kunden, die mehrere Karten besitzen können
- Transaktionen, die über Karten ausgelöst werden
- Händler und Adressen, denen Transaktionen zugeordnet sind
- stg_wechselkurse als normalisierte Wechselkurstabelle
- stg_ungueltige_adresse als Kontrolltabelle für fehlerhafte Datensätze
Diese Schicht dient der Datenbereinigung, Validierung und Vorbereitung für das Data‑Warehouse.
![ERM_Staging.db.png](Dokumentation/ERM_Staging.db.png)

 Fazit
Dieses Projekt demonstriert, wie umfangreiche Kreditkartendaten aus mehreren Kaggle‑Datensätzen zu einem vollständigen, analytisch nutzbaren Data‑Warehouse verarbeitet werden können.
Durch die Kombination aus Datenbereinigung, Validierung, Staging‑Modellierung und der Überführung in ein strukturiertes Star‑Schema entsteht ein robustes System, das reale Data‑Engineering‑Prozesse abbildet.
Die implementierten ETL‑Pipelines stellen sicher, dass Datenqualität gewährleistet wird, während SCD‑Mechanismen und Wechselkurslogik eine realistische Verarbeitung ermöglichen.
Auf Basis des Data‑Warehouses können anschließend aussagekräftige Analysen durchgeführt werden, darunter Betrugserkennung, Umsatzanalysen, Händlerverhalten und zeitliche Trends.
Damit zeigt das Projekt, wie ein modernes, skalierbares und praxisnahes Data‑Engineering‑System aufgebaut wird — von der Rohdatenintegration bis zur analytischen Auswertung.
