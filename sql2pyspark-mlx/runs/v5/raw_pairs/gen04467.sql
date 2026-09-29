SELECT
    status,
    CASE WHEN monthly_fee >= 190 THEN 'high' WHEN monthly_fee >= 99 THEN 'mid' ELSE 'low' END AS monthly_fee_bucket,
    MIN(monthly_fee) AS min_monthly_fee,
    ROUND(AVG(monthly_fee), 2) AS avg_monthly_fee
FROM subscriptions
WHERE monthly_fee <= 180 AND end_date BETWEEN '2024-10-10' AND '2025-10-01'
GROUP BY 1, 2
ORDER BY avg_monthly_fee;
