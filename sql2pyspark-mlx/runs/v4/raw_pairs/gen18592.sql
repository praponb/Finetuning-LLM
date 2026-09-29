WITH ranked AS (
    SELECT
        id,
        status,
        amount,
        ROW_NUMBER() OVER (PARTITION BY status ORDER BY amount, id) rn
    FROM orders
    WHERE discount <= 0.25
)
SELECT id, status, amount
FROM ranked
WHERE rn <= 3;
