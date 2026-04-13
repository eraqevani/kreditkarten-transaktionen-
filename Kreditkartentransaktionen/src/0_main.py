import runpy

def run_script(script_name):
    print(f"\n Starte: {script_name}")
    runpy.run_path(script_name)
    print(f" Fertig: {script_name}")

def main():
    print("========== DATA PIPELINE START ==========")

    # -------------------------
    # 1. SOURCE LAYER
    # -------------------------
    run_script("1_source_db_erstellen.py")

    # -------------------------
    # 2. STAGING LAYER
    # -------------------------
    run_script("2_staging_db_erstellen.py")
    run_script("3_etl_daten_aus_creditcard_in_staging.py")

    # -------------------------
    # 3. DWH LAYER
    # -------------------------
    run_script("4_dwh_erstellung.py")
    run_script("5_etl_daten_aus_staging_in_dwh.py")

    # -------------------------
    # 4. SCD UPDATE
    # -------------------------
    run_script("6_scd_scenario.py")

    # -------------------------
    # 5. OLAP / ANALYTICS
    # -------------------------
    run_script("7_olaps.py")

    print("\n PIPELINE ERFOLGREICH DURCHGELAUFEN")

if __name__ == "__main__":
    main()