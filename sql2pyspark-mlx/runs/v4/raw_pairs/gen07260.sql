WITH Ranked AS (
    SELECT
        id,
        customer_id,
        amount,
        RANK() OVER (PARTITION BY customer_id ORDER BY amount DESC) AS pos
    FROM orders
)
SELECT id, customer_id, amount
FROM Ranked
WHERE pos = 1
ORDER BY customer_id DESC
