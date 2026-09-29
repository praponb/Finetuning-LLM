WITH filtered_orders AS (
    SELECT *, DATE_TRUNC('WEEK', order_date) AS order_date_period
    FROM orders
    WHERE store_id IN (5, 15, 17, 30)
),
AmountBucketSummary AS (
    SELECT
        CASE WHEN amount >= 1280 THEN 'high' WHEN amount >= 380 THEN 'mid' ELSE 'low' END AS amount_bucket,
        YEAR(order_date) AS order_year,
        ROUND(AVG(amount), 2) AS avg_amount,
        MIN(amount) AS min_amount
    FROM filtered_orders
    GROUP BY CASE WHEN amount >= 1280 THEN 'high' WHEN amount >= 380 THEN 'mid' ELSE 'low' END, YEAR(order_date)
)
SELECT
    amount_bucket,
    order_year,
    avg_amount,
    min_amount
FROM AmountBucketSummary;
