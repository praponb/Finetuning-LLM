WITH src AS (
    SELECT *, LOWER(status) AS status_lower
    FROM orders
    WHERE NOT ((shipping_fee >= 13 AND amount < 1330) OR customer_id = 2)
),
agg AS (
    SELECT customer_id, AVG(discount) AS avg_discount, COUNT(amount) AS amount_count
    FROM src
    GROUP BY customer_id
)
SELECT customer_id, avg_discount, amount_count
FROM agg
ORDER BY amount_count DESC
