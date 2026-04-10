# Painting Production Plan 2026

One-time ETL pipeline that extracts and transforms the 2026 painting production plan from a Hevo-ingested Excel source table in DigitalOcean Managed PostgreSQL, and loads it into a clean analytical table.

## Context

The source data is an Excel production plan imported into PostgreSQL via Hevo Data. The raw table has a hierarchical structure where week boundaries and machine/resource sections are embedded as header rows in the `tellija` column rather than as separate metadata columns. This pipeline normalises that structure.

## Data sources

| Source | Description |
|--------|-------------|
| `public.painting_work_plan_2026_na_as_h_vl_v_rv_ja_a_r_hm_to_v_rv_to_20` | Hevo-ingested Excel painting production plan for 2026 |
| `public.contract_delivery_list_filled_query_results` | Contract master data used to fill in missing trader (haldur) information |

## Output table

**`public.production_plan_painting`**

| Column | Type | Description |
|--------|------|-------------|
| `id` | SERIAL | Auto-incremented primary key |
| `status` | VARCHAR | Production status (e.g. Valmis, VABA) |
| `customer` | VARCHAR | Customer name (`tellija`) |
| `trader` | VARCHAR | Responsible trader / haldur |
| `running_meters` | NUMERIC(12,2) | Masindamine in metres |
| `planned_production_time_h` | NUMERIC(10,2) | Planned production time in hours |
| `contract_number` | VARCHAR | Cleaned contract/offer number |
| `equipment` | VARCHAR | Machine used (A250, IP250, Sprei) |
| `week_label` | VARCHAR | Week identifier (N2–N17) |
| `resource` | VARCHAR | Resource type (Harjasmasin / Spreiliin) |

## Transformations applied

- **Week labelling** — rows in the source are grouped by week using `Tootmise algus N{n}` header rows. A `week_label` column (N2, N3, … N17) is propagated to all data rows within each week block using a window function island technique.
- **Resource labelling** — `HARJASMASIN` and `SPREILIIN` section header rows are used to derive a `resource` column for each data row in the same way.
- **Trader fill** — where `trader` is missing in the source, it is filled from `contract_delivery_list_filled_query_results` by joining on the cleaned contract number.
- **Contract number cleaning** — suffixes like `(7)` are stripped (e.g. `6695 (7)` → `6695`).
- **Contract number splitting** — rows with two contract numbers separated by `/` (e.g. `103667/ 103681`) are expanded into two rows with `running_meters` and `planned_production_time_h` split evenly.
- **Name normalisation** — trader names and resource labels are converted to title case.
- **Rounding** — `running_meters` and `planned_production_time_h` are rounded to 2 decimal places.

## Running the load script

Requires Python 3 with `psycopg2` installed:

```bash
pip install psycopg2-binary
python load_production_plan.py
```

Connection parameters are read directly from the script and require access to the DigitalOcean PostgreSQL cluster. For production use, move credentials to environment variables or a `.env` file.

The script is **idempotent** — it drops and recreates the target table on each run (`DROP TABLE IF EXISTS`).

After loading, `SELECT` is granted to the `doadmin` user (used by Metabase / read-only consumers).
