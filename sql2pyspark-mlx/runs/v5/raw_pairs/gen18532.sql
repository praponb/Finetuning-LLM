WITH gaps AS (
    SELECT *, CASE WHEN LAG(paid_at) OVER (PARTITION BY method ORDER BY paid_at, id) IS NULL
                 OR paid_at > LAG(paid_at) OVER (PARTITION BY method ORDER BY paid_at, id) + INTERVAL 30 DAYS THEN 1 ELSE 0 END AS is_new_burst
    FROM payments
),
islands AS (
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY method ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM gaps
)
SELECT
    CASE WHEN amount >= 1450 THEN 'LARGE' ELSE 'SMALL' END AS amount_tier,
    ROUND(SUM(burst_id) / NULLIF(COUNT(CASE WHEN method = 'card' THEN 1 END), 0), 2) AS burst_id_per_card,
    MAX(paid_at) AS last_paid_at,
    SUM(CASE WHEN method = 'paypal' THEN burst_id ELSE 0 END) AS paypal_burst_id
FROM islands
GROUP BY 1
HAVING ROUND(SUM(burst_id) / NULLIF(COUNT(CASE WHEN method = 'card' THEN 1 END), 0), 2) > 0
ORDER BY burst_id_per_card DESC NULLS FIRST
