WITH ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY quantity DESC, id) AS rn
    FROM inventory
    WHERE quantity > 99
)
SELECT *
FROM ranked
WHERE rn <= 2
