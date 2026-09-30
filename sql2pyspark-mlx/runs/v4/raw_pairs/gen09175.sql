WITH gaps AS (
    SELECT *, CASE WHEN paid_at - LAG(paid_at) OVER (PARTITION BY order_id ORDER BY paid_at, id) > INTERVAL '90' DAY THEN 1 ELSE 0 END AS is_new_burst
    FROM payments
),
islands AS (
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY order_id ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM gaps
)
SELECT
    status,
    order_id,
    COUNT(*) FILTER (WHERE method = 'bank_transfer') AS bank_transfer_rows,
    APPROX_PERCENTILE(island_id, 0.9) AS island_id_p90
FROM islands
GROUP BY status, order_id
