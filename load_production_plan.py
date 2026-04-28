import os
import psycopg2
import re

conn = psycopg2.connect(
    host=os.environ.get('PG_HOST', 'localhost'),
    port=int(os.environ.get('PG_PORT', 5432)),
    user=os.environ.get('PG_USER', 'user'),
    password=os.environ.get('PG_PASSWORD', ''),
    dbname=os.environ.get('PG_DATABASE', 'defaultdb'),
    sslmode='require'
)
cur = conn.cursor()

# --- 1. Fetch and transform data ---

SQL = """
WITH ordered AS (
    SELECT
        a, tellija, haldur, masindamine_m, planeeritud_tootmisaeg_h,
        lepingu_number, masin, __hevo_id,
        CASE
            WHEN tellija ~ '^Tootmise algus N[0-9]+'
            THEN regexp_replace(tellija, '^Tootmise algus (N[0-9]+).*', '\\1')
            ELSE NULL
        END AS week_marker,
        CASE
            WHEN upper(tellija) ~ '^SPREILIIN' THEN 'Spreiliin'
            WHEN upper(tellija) ~ '^HARJASMASIN' THEN 'Harjasmasin'
            ELSE NULL
        END AS resource_marker
    FROM public.painting_work_plan_2026_na_as_h_vl_v_rv_ja_a_r_hm_to_v_rv_to_20
),
week_grouped AS (
    SELECT *,
        COUNT(week_marker) OVER (ORDER BY __hevo_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS week_grp,
        COUNT(resource_marker) OVER (ORDER BY __hevo_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS res_grp
    FROM ordered
),
labeled AS (
    SELECT *,
        FIRST_VALUE(week_marker) OVER (PARTITION BY week_grp ORDER BY __hevo_id) AS week_label,
        FIRST_VALUE(resource_marker) OVER (PARTITION BY res_grp ORDER BY __hevo_id) AS resource
    FROM week_grouped
),
contracts AS (
    SELECT DISTINCT
        ltrim(contract_number, '0') AS contract_num,
        initcap(trader) AS trader
    FROM public.contract_delivery_list_filled_query_results
    WHERE contract_number ~ '^[0-9]+'
)
SELECT
    l.a AS status,
    l.tellija AS customer,
    COALESCE(initcap(l.haldur), c.trader) AS trader,
    l.masindamine_m AS running_meters,
    l.planeeritud_tootmisaeg_h AS planned_production_time_h,
    l.lepingu_number AS contract_number,
    l.masin AS equipment,
    l.week_label,
    l.resource
FROM labeled l
LEFT JOIN contracts c
    ON ltrim(regexp_replace(l.lepingu_number, '\\s*\\([^)]*\\)\\s*$', ''), '0')
       = c.contract_num
WHERE l.lepingu_number IS NOT NULL
ORDER BY l.__hevo_id;
"""

cur.execute(SQL)
raw_rows = cur.fetchall()
cols = [d[0] for d in cur.description]


def clean_contract(val):
    val = val.strip()
    val = re.sub(r'\s*\([^)]*\)\s*$', '', val).strip()
    return val


def split_row(d):
    raw = d['contract_number']
    if '/' in raw:
        parts = [clean_contract(p) for p in raw.split('/') if p.strip()]
        n = len(parts)
        out = []
        for p in parts:
            r = dict(d)
            r['contract_number'] = p
            if d['running_meters'] is not None:
                r['running_meters'] = round(d['running_meters'] / n, 2)
            if d['planned_production_time_h'] is not None:
                r['planned_production_time_h'] = round(d['planned_production_time_h'] / n, 2)
            out.append(r)
        return out
    else:
        r = dict(d)
        r['contract_number'] = clean_contract(raw)
        if r['running_meters'] is not None:
            r['running_meters'] = round(r['running_meters'], 2)
        if r['planned_production_time_h'] is not None:
            r['planned_production_time_h'] = round(r['planned_production_time_h'], 2)
        return [r]


output_cols = ['status', 'customer', 'trader', 'running_meters',
               'planned_production_time_h', 'contract_number',
               'equipment', 'week_label', 'resource']

output_rows = []
for row in raw_rows:
    d = dict(zip(cols, row))
    for r in split_row(d):
        output_rows.append(tuple(r[c] for c in output_cols))

print(f"Rows to load: {len(output_rows)}")

# --- 2. Create target table and load data ---

cur.execute("DROP TABLE IF EXISTS public.production_plan_painting;")

cur.execute("""
    CREATE TABLE public.production_plan_painting (
        id                      SERIAL PRIMARY KEY,
        status                  VARCHAR(512),
        customer                VARCHAR(512),
        trader                  VARCHAR(512),
        running_meters          NUMERIC(12, 2),
        planned_production_time_h NUMERIC(10, 2),
        contract_number         VARCHAR(100),
        equipment               VARCHAR(512),
        week_label              VARCHAR(10),
        resource                VARCHAR(50)
    );
""")

insert_sql = """
    INSERT INTO public.production_plan_painting
        (status, customer, trader, running_meters, planned_production_time_h,
         contract_number, equipment, week_label, resource)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
"""

cur.executemany(insert_sql, output_rows)

cur.execute("GRANT SELECT ON public.production_plan_painting TO doadmin;")

conn.commit()
print("Table public.production_plan_painting created and loaded.")
print(f"Rows inserted: {len(output_rows)}")

cur.close()
conn.close()
