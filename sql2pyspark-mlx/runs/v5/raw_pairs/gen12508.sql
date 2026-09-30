WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        DATE_TRUNC('MONTH', delivered_date) AS delivered_date_period,
        COALESCE(cost, 0) AS cost_filled,
        COALESCE(carrier, 'UNKNOWN') AS carrier_filled
    FROM shipments
    WHERE shipped_date BETWEEN '2023-03-31' AND '2025-08-01'
),
flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN delivered_date_period - LAG(delivered_date_period) OVER (PARTITION BY carrier ORDER BY delivered_date_period, id) > INTERVAL 60 DAYS THEN 1 ELSE 0 END AS new_group_flag
    FROM cleaned
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_group_flag) OVER (PARTITION BY carrier ORDER BY delivered_date_period, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM flagged
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LEAD(cost, 1, 0) OVER w_running AS next_cost,
        LAG(cost, 1) OVER (PARTITION BY status ORDER BY cost_filled, id) AS prev_cost
    FROM sessionized
    WHERE delivered_date_period >= '2025-10-09'
    WINDOW w_running AS (PARTITION BY order_id ORDER BY delivered_date, id)
),
with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN delivered_date_period - LAG(delivered_date_period) OVER (PARTITION BY order_id ORDER BY delivered_date_period, id) > INTERVAL 30 DAYS THEN 1 ELSE 0 END AS is_new_burst
    FROM scored
),
sessions_shipments AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        order_id,
        carrier,
        delivered_date_period,
        carrier_filled,
        next_cost,
        prev_cost,
        SUM(is_new_burst) OVER (PARTITION BY order_id ORDER BY delivered_date_period, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM with_gaps
)
SELECT
    id,
    carrier,
    delivered_date_period,
    prev_cost
FROM sessions_shipments
ORDER BY id DESC NULLS LAST;
