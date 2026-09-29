SELECT
    channel,
    CASE WHEN amount >= 1230 THEN 'high' WHEN amount >= 250 THEN 'mid' ELSE 'low' END AS amount_bucket,
    CASE WHEN YEAR(order_date) < 2024 THEN 'before_2024' WHEN YEAR(order_date) = 2024 THEN '2024' ELSE 'after_2024' END AS order_cohort,
    SUM(CASE WHEN status = 'PENDING' THEN amount ELSE 0 END) AS pending_amount
FROM orders
WHERE customer_id = 7 AND store_id IS NOT NULL
GROUP BY channel, amount_bucket, order_cohort
ORDER BY pending_amount DESC;
