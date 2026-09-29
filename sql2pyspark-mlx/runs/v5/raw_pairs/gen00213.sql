WITH orders_by_customer AS (
    SELECT customer_id, MIN(discount) AS min_discount
    FROM orders
    WHERE channel IN ('mobile', 'partner')
    GROUP BY customer_id
)
SELECT
    c.id,
    c.segment,
    c.city,
    obc.min_discount
FROM customers c
INNER JOIN orders_by_customer obc ON c.id = obc.customer_id
WHERE obc.min_discount >= 0.21
ORDER BY city;
