WITH latest AS (
    SELECT *
    FROM payments
    QUALIFY ROW_NUMBER() OVER (PARTITION BY status ORDER BY amount, id) = 1
)
SELECT
    status,
    CASE WHEN amount >= 1580 THEN 'HIGH' ELSE 'LOW' END AS amount_tier,
    APPROX_PERCENTILE(amount, ARRAY(0.25, 0.5, 0.75)) AS amount_quartiles,
    STDDEV_SAMP(amount) AS amount_stddev,
    SIZE(COLLECT_SET(method)) AS num_methods,
    APPROX_PERCENTILE(amount, ARRAY(0.25, 0.5, 0.75)) AS amount_quartiles_2,
    SUM(CASE WHEN method = 'cash' THEN amount ELSE 0 END) AS cash_amount,
    MAX(paid_at) AS last_paid_at
FROM latest
GROUP BY CUBE (status, CASE WHEN amount >= 1580 THEN 'HIGH' ELSE 'LOW' END)
HAVING SUM(CASE WHEN method = 'cash' THEN amount ELSE 0 END) < 0
ORDER BY status;
