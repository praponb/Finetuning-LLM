WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        COALESCE(carrier, 'unassigned') AS carrier_filled,
        ROUND(cost, 1) AS cost_rounded
    FROM shipments
    WHERE order_id IN (2, 38)
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        LAG(cost_rounded) OVER w_hist AS prev_cost_rounded,
        LEAD(cost_rounded) OVER w_hist AS next_cost_rounded,
        LEAD(cost) OVER w_hist AS next_cost,
        LAG(cost) OVER w_hist AS prev_cost,
        ROW_NUMBER() OVER w_hist AS event_seq,
        ROW_NUMBER() OVER w_hist_2 AS row_seq
    FROM filtered
    WINDOW w_hist AS (PARTITION BY status ORDER BY shipped_date, id), w_hist_2 AS (PARTITION BY status ORDER BY delivered_date DESC, id)
),
with_windows_shipments AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY prev_cost_rounded DESC, id) AS row_seq_2,
        LAST_VALUE(carrier) IGNORE NULLS OVER (ORDER BY event_seq, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_carrier,
        ROW_NUMBER() OVER w_running AS row_seq_3
    FROM with_windows
    WINDOW w_running AS (PARTITION BY order_id ORDER BY cost DESC, id)
),
matched_keys AS (
    -- Set intersection on keys
    SELECT carrier, status
    FROM with_windows_shipments
    INTERSECT ALL
    SELECT carrier, status
    FROM shipments
    WHERE order_id IN (6, 11, 15)
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM with_windows_shipments s
    INNER JOIN matched_keys k ON s.carrier = k.carrier AND s.status = k.status
),
with_windows_shipments_2 AS (
    -- Window metrics over several different partitions
    SELECT *, CUME_DIST() OVER (ORDER BY event_seq DESC NULLS FIRST) AS event_seq_cume_dist
    FROM verified
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        carrier,
        delivered_date,
        cost,
        status,
        carrier_filled,
        cost_rounded,
        prev_cost_rounded,
        next_cost_rounded,
        next_cost,
        event_seq,
        row_seq,
        event_seq_cume_dist,
        ROUND(event_seq * 1.07, 2) AS adjusted_event_seq,
        COALESCE(carrier, 'DIRECT') AS carrier_filled_2
    FROM with_windows_shipments_2
    WHERE order_id IS NOT NULL
      AND carrier IS NOT DISTINCT FROM 'Kerry'
),
base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        carrier,
        delivered_date,
        cost,
        status,
        cost_rounded,
        prev_cost_rounded,
        next_cost_rounded,
        next_cost,
        event_seq,
        row_seq,
        event_seq_cume_dist,
        ROUND(event_seq_cume_dist, 0) AS event_seq_cume_dist_rounded,
        COALESCE(carrier, 'NONE') AS carrier_clean
    FROM valid
    WHERE delivered_date >= DATE '2023-08-17'
)
-- Multi-level aggregation
SELECT
    DATE_TRUNC('QUARTER', delivered_date) AS quarter_start,
    carrier,
    COUNT(*) AS cnt,
    COUNT(DISTINCT status) AS distinct_status,
    MAX(cost) AS max_cost,
    AVG(event_seq_cume_dist_rounded) AS avg_event_seq_cume_dist_rounded,
    COUNT(*) FILTER (WHERE status = 'IN_TRANSIT') AS in_transit_rows
FROM base
GROUP BY GROUPING SETS ((DATE_TRUNC('QUARTER', delivered_date), carrier), (DATE_TRUNC('QUARTER', delivered_date)), ())
HAVING COUNT(*) FILTER (WHERE status = 'IN_TRANSIT') >= 1
ORDER BY quarter_start NULLS LAST
