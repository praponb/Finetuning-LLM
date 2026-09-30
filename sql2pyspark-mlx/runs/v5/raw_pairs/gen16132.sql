WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CONCAT(carrier, '/', status) AS carrier_status,
        COALESCE(carrier, 'OTHER') AS resolved_carrier
    FROM shipments
    WHERE delivered_date > DATE '2023-06-02'
      AND order_id IS NOT NULL
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        COALESCE(cost, 0) AS cost_filled,
        DATE_TRUNC('YEAR', shipped_date) AS shipped_date_period
    FROM filtered
    WHERE delivered_date > DATE '2023-08-29'
      AND carrier IS NOT NULL
),
with_prev AS (
    -- Previous event time per key
    SELECT *, LAG(shipped_date_period) OVER (PARTITION BY status ORDER BY shipped_date_period, id) AS prev_shipped_date_period
    FROM cleaned
),
gaps AS (
    -- Flag rows that start a new session
    SELECT *, CASE WHEN prev_shipped_date_period IS NULL OR shipped_date_period > prev_shipped_date_period + INTERVAL '7' DAY THEN 1 ELSE 0 END AS is_new_burst
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY status ORDER BY shipped_date_period, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM gaps
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        order_id,
        shipped_date,
        delivered_date,
        cost,
        status,
        carrier_status,
        prev_shipped_date_period,
        island_id,
        AVG(island_id) OVER (ORDER BY shipped_date_period, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS island_id_avg_5,
        ROW_NUMBER() OVER (PARTITION BY status ORDER BY cost_filled, id) AS row_seq,
        SUM(cost_filled) OVER (ORDER BY shipped_date_period, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_cost_filled
    FROM sessionized
)
-- Join a dimension and aggregate
SELECT
    d.channel,
    s.status AS shipment_status,
    COUNT(*) AS cnt,
    ROUND(SUM(s.row_seq), 2) AS total_row_seq,
    MIN(s.island_id) AS min_island_id
FROM with_windows s
LEFT JOIN orders d ON s.order_id = d.id
GROUP BY ROLLUP (d.channel, s.status)
