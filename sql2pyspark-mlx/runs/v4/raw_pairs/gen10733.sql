WITH recent_orders AS (
    SELECT *, FLOOR(amount) AS amount_whole
    FROM orders
    WHERE amount > 1110
),
orders_stats AS (
    SELECT YEAR(order_date) AS order_year, COUNT(CASE WHEN status = 'CANCELLED' THEN 1 END) AS cancelled_count
    FROM recent_orders
    GROUP BY YEAR(order_date)
)
SELECT *
FROM orders_stats
WHERE cancelled_count < 8
ORDER BY 1 DESC;
