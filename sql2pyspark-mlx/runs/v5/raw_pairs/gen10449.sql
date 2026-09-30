WITH src AS (
    SELECT *
    FROM orders
    WHERE shipping_fee <= 28 OR shipping_fee BETWEEN 22 AND 29
),
Agg AS (
    SELECT QUARTER(order_date) AS order_quarter, SUM(shipping_fee) AS total_shipping_fee, SUM(amount) AS total_amount
    FROM src
    GROUP BY QUARTER(order_date)
)
SELECT *
FROM Agg
WHERE total_shipping_fee < 150
ORDER BY 2 DESC
