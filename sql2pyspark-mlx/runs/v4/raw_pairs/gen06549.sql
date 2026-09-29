SELECT
    DATE_FORMAT(order_date, 'yyyy-MM') AS order_month,
    CASE WHEN YEAR(order_date) < 2024 THEN 'before_2024' WHEN YEAR(order_date) = 2024 THEN '2024' ELSE 'after_2024' END AS order_cohort,
    CASE WHEN amount >= 1340 THEN 'high' WHEN amount >= 330 THEN 'mid' ELSE 'low' END AS amount_bucket,
    COUNT(amount) AS amount_count,
    COUNT(DISTINCT amount) AS unique_statuss,
    COUNT(*) AS row_count
FROM orders
WHERE amount >= 1110 AND shipping_fee BETWEEN 15 AND 18 AND shipping_fee >= 25
GROUP BY DATE_FORMAT(order_date, 'yyyy-MM'), CASE WHEN YEAR(order_date) < 2024 THEN 'before_2024' WHEN YEAR(order_date) = 2024 THEN '2024' ELSE 'after_2024' END, CASE WHEN amount >= 1340 THEN 'high' WHEN amount >= 330 THEN 'mid' ELSE 'low' END;
