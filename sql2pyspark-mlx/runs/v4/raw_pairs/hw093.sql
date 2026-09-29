WITH high_value AS (
    SELECT customer_id, SUM(amount) AS total
    FROM orders
    GROUP BY customer_id
    HAVING SUM(amount) > 2000
)
SELECT * FROM high_value;
