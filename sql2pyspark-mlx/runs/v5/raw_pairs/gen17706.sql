WITH filtered AS (
    SELECT *, HOUR(paid_at) AS paid_at_hour
    FROM payments
    WHERE order_id IN (13, 25, 40)
),
with_prev AS (
    SELECT *, LAG(paid_at) OVER (PARTITION BY order_id ORDER BY paid_at, id) AS prev_paid_at
    FROM filtered
),
flagged AS (
    SELECT *, CASE WHEN prev_paid_at IS NULL OR paid_at > prev_paid_at + INTERVAL '90' DAY THEN 1 ELSE 0 END AS new_group_flag
    FROM with_prev
),
islands AS (
    SELECT *, SUM(new_group_flag) OVER (PARTITION BY order_id ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM flagged
),
scored AS (
    SELECT
        *,
        LEAD(island_id, 1) OVER (PARTITION BY status ORDER BY prev_paid_at, id) AS next_island_id,
        ROW_NUMBER() OVER w_running AS row_seq
    FROM islands
    WINDOW w_running AS (PARTITION BY status, order_id ORDER BY prev_paid_at DESC, id)
),
valid AS (
    SELECT
        *,
        ROUND(row_seq, 0) AS row_seq_rounded,
        next_island_id * paid_at_hour AS next_island_id_times_paid_at_hour,
        COALESCE(method, 'N/A') AS method_filled
    FROM scored
    WHERE paid_at > DATE '2023-08-30'
      AND method = 'promptpay'
),
ranked AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY status, order_id ORDER BY amount, id) AS rn
        FROM valid
    ) x
    WHERE rn = 1
)
SELECT
    DATE_TRUNC('MONTH', paid_at) AS activity_month,
    COUNT(*) AS row_count,
    APPROX_PERCENTILE(island_id, ARRAY(0.25, 0.5, 0.75)) AS island_id_quartiles,
    SUM(CASE WHEN status = 'REFUNDED' THEN paid_at_hour ELSE 0 END) AS refunded_paid_at_hour
FROM ranked
GROUP BY DATE_TRUNC('MONTH', paid_at)
HAVING COUNT(*) < 1000;
