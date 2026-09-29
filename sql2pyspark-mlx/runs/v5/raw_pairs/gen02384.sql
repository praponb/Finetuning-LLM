WITH base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        carrier,
        shipped_date,
        delivered_date,
        cost,
        QUARTER(delivered_date) AS delivered_date_quarter,
        INITCAP(TRIM(status)) AS status_title
    FROM shipments
    WHERE carrier IS DISTINCT FROM 'Flash'
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY carrier ORDER BY delivered_date_quarter NULLS FIRST, id) AS rank_in_group
    FROM base
    QUALIFY rank_in_group = 1
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        NTILE(3) OVER w_grp AS cost_bucket,
        ROW_NUMBER() OVER w_ordered AS seq_no,
        AVG(cost) OVER (PARTITION BY carrier) AS carrier_avg_cost
    FROM ranked
    WINDOW w_grp AS (PARTITION BY carrier ORDER BY cost, id), w_ordered AS (PARTITION BY carrier ORDER BY rank_in_group DESC, id)
),
matched_keys AS (
    -- Set intersection on keys
    SELECT id, carrier
    FROM with_windows
    INTERSECT
    SELECT id, carrier
    FROM shipments
    WHERE order_id IS NOT NULL
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM with_windows s
    INNER JOIN matched_keys k ON s.id = k.id AND s.carrier = k.carrier
)
SELECT
    id,
    carrier,
    delivered_date,
    cost,
    status_title,
    rank_in_group,
    cost_bucket,
    seq_no,
    carrier_avg_cost,
    CASE WHEN carrier RLIKE '^(UPS|DHL)$' THEN 'GROUP_A' WHEN carrier RLIKE '^(FedEx|Kerry)$' THEN 'PRIORITY' ELSE 'GROUP_B' END AS carrier_zone
FROM kept_rows
ORDER BY carrier DESC;
