WITH base AS (
    SELECT *
    FROM orders
    WHERE status = 'CANCELLED' AND shipping_fee BETWEEN 14 AND 26
),
orders_stats AS (
    SELECT QUARTER(order_date) order_quarter, YEAR(order_date) order_year, COUNT(shipping_fee) shipping_fee_count
    FROM base
    GROUP BY QUARTER(order_date), YEAR(order_date)
)
SELECT order_quarter, order_year, shipping_fee_count
FROM orders_stats
ORDER BY order_quarter, order_year DESC
LIMIT 10;
