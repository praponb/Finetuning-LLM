SELECT
    CASE WHEN amount >= 4400 THEN 'PREMIUM' ELSE 'STANDARD' END AS amount_tier,
    ROUND(AVG(amount), 2) AS avg_amount,
    APPROX_PERCENTILE(amount, 0.9) AS amount_p90,
    ROUND(AVG(amount), 2) AS avg_amount_2
FROM transactions
GROUP BY 1
HAVING avg_amount_2 >= 0
