SELECT
    channel,
    CASE WHEN amount >= 1670 THEN 'high' WHEN amount >= 580 THEN 'mid' ELSE 'low' END AS amount_bucket,
    status,
    MIN(order_date) AS min_order_date
FROM orders
WHERE customer_id IS NULL
GROUP BY channel, amount_bucket, status;
