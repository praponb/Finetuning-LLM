WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        FLOOR(cost) AS cost_whole,
        COALESCE(carrier, 'DIRECT') AS carrier_filled
    FROM shipments
    WHERE shipped_date > TO_DATE('2023-08-15')
      AND cost >= 42
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        carrier,
        shipped_date,
        delivered_date,
        cost,
        status,
        carrier_filled,
        LAST_VALUE(cost) IGNORE NULLS OVER (ORDER BY delivered_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_cost,
        LAG(cost, 2) OVER (ORDER BY shipped_date, id) AS prev_cost,
        ROW_NUMBER() OVER (PARTITION BY status ORDER BY delivered_date DESC, id) AS row_seq,
        AVG(cost_whole) OVER (PARTITION BY carrier) AS carrier_avg_cost_whole,
        LEAD(cost_whole, 1) OVER (PARTITION BY status ORDER BY cost, id) AS next_cost_whole
    FROM filtered
    WHERE cost_whole != 29
),
with_windows_shipments AS (
    -- Running and moving aggregates
    SELECT
        *,
        CUME_DIST() OVER (ORDER BY next_cost_whole NULLS LAST) AS next_cost_whole_cume_dist,
        NTILE(5) OVER (ORDER BY row_seq, id) AS row_seq_bucket,
        ROW_NUMBER() OVER (PARTITION BY carrier ORDER BY carrier_avg_cost_whole DESC, id) AS row_seq_2
    FROM with_windows
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        last_known_cost / NULLIF(next_cost_whole_cume_dist, 0) AS last_known_cost_per_next_cost_whole_cume_dist,
        COALESCE(carrier, 'DIRECT') AS carrier_clean
    FROM with_windows_shipments
    WHERE shipped_date >= DATE '2023-08-24'
      AND carrier IS NOT NULL
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY status, carrier ORDER BY cost NULLS FIRST) AS latest_rank
        FROM valid
    ) ranked
    WHERE latest_rank = 1
),
base AS (
    -- Keep recent, valid rows
    SELECT
        *,
        carrier_avg_cost_whole * last_known_cost_per_next_cost_whole_cume_dist AS carrier_avg_cost_whole_times_last_known_cost_per_next_cost_whole_cume_dist,
        COALESCE(carrier, 'UNKNOWN') AS resolved_carrier
    FROM ranked
    WHERE prev_cost <= 96
)
-- Native SQL UNPIVOT (wide to long)
SELECT *
FROM (
    SELECT
        id,
        status,
        prev_cost,
        last_known_cost_per_next_cost_whole_cume_dist,
        carrier_avg_cost_whole_times_last_known_cost_per_next_cost_whole_cume_dist
    FROM base
) src
UNPIVOT INCLUDE NULLS (
    val FOR metric IN (prev_cost, last_known_cost_per_next_cost_whole_cume_dist, carrier_avg_cost_whole_times_last_known_cost_per_next_cost_whole_cume_dist)
);
