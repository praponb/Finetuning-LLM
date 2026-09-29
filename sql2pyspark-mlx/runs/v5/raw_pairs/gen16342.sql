WITH with_dims AS (
    -- Attach accounts attributes
    SELECT a.*, d.opened_date
    FROM transactions a
    LEFT JOIN accounts d
        ON a.account_id = d.id
    WHERE d.balance < 19000
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN amount >= 4000 THEN 'HIGH' ELSE 'LOW' END AS amount_tier,
    STDDEV_SAMP(amount) AS amount_stddev,
    ROUND(SUM(amount) / NULLIF(COUNT(CASE WHEN txn_type = 'FEE' THEN 1 END), 0), 2) AS amount_per_fee,
    PERCENTILE_APPROX(amount, 0.9) AS amount_p90,
    MAX(amount) FILTER (WHERE merchant = '7-Eleven') AS 7_eleven_max_amount,
    SORT_ARRAY(COLLECT_SET(merchant)) AS merchant_list,
    STDDEV_SAMP(amount) AS amount_stddev_2
FROM with_dims
GROUP BY amount_tier
HAVING 7_eleven_max_amount >= 0
