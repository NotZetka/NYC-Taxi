# NYC Taxi medallion ELT

Projekt zaliczeniowy: sciagam miesieczne dane Yellow Taxi z TLC, laduje je do DuckDB i robie trzy warstwy bronze/silver/gold.

## Pliki
- `scripts/run_pipeline.py` - prosty skrypt, ktory pobiera pliki i odpala SQL.
- `sql/elt_pipeline.sql` - wszystkie CREATE TABLE i transformacje.
- `docs/` - opis problemu, architektura i notatka o jakosci danych.
- `data/` - katalog na pobrane pliki i baze DuckDB (nie commitowane).

## Uruchomienie
```bash
python -m venv .venv
. .venv/Scripts/activate 
pip install -r requirements.txt
python scripts/run_pipeline.py --refresh
```
Skrypt sciaga ok. 10 GB Parquetow (2025-01..08) + `taxi_zone_lookup.csv`, tworzy `data/nyc_taxi.duckdb` i odpala SQL-a.

## Co wychodzi
- `bronze.yellow_tripdata` - surowe rekordy z Parquetow.
- `silver.yellow_tripdata_clean` - oczyszczone dane + metryki (np. `trip_minutes`).
- `gold.zone_hourly_metrics` oraz `gold.payment_type_metrics` - agregaty pod BI.

Sprawdzenie liczby rekordow:
```powershell
@"
import duckdb
con = duckdb.connect("data/nyc_taxi.duckdb")
print(con.execute("SELECT COUNT(*) FROM gold.zone_hourly_metrics").fetchone()[0])
"@ 
```
