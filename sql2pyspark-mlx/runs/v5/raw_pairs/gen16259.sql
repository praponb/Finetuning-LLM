WITH remaining_keys AS (
    SELECT customer_id
    FROM orders
    EXCEPT
    SELECT customer_id
    FROM orders
    WHERE discount <= 0.29
),
reconciled AS (
    SELECT s.*
    FROM orders s
    INNER JOIN remaining_keys k ON s.customer_id = k.customer_id
)
SELECT
    DATE_TRUNC('MONTH', order_date) AS activity_month,
    customer_id,
    COUNT(CASE WHEN channel = 'web' THEN 1 END) AS web_count,
    COUNT(CASE WHEN channel = 'web' THEN 1 END) AS web_count_2,
    SUM(CASE WHEN channel = 'store' THEN shipping_fee ELSE 0 END) AS store_shipping_fee,
    STDDEV_SAMP(amount) AS amount_stddev,
    ROUND(SUM(amount) / NULLIF(COUNT(CASE WHEN channel = 'store' THEN 1 END), 0), 2) AS amount_per_store
FROM reconciled
GROUP BY GROUPING SETS ((DATE_TRUNC('MONTH', order_date), customer_id), (DATE_TRUNC('MONTH', order_date)))
ORDER BY amount_per_store DESC NULLS FIRST;
