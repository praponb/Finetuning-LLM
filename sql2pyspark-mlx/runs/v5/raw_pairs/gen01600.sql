WITH filtered_orders AS (
    SELECT *, DATE_FORMAT(order_date, 'MMM yyyy') AS order_date_label
    FROM orders
    WHERE order_date <= '2024-07-25'
),
agg AS (
    SELECT DATE_FORMAT(order_date, 'yyyy-MM') AS order_month, MIN(order_date) AS first_order_date, ROUND(AVG(shipping_fee), 2) AS avg_shipping_fee
    FROM filtered_orders
    GROUP BY DATE_FORMAT(order_date, 'yyyy-MM')
)
SELECT *
FROM agg
WHERE avg_shipping_fee >= 35
ORDER BY order_month
