WITH ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY quantity DESC, id) AS rnk
    FROM inventory
)
SELECT id, store_id, quantity
FROM ranked
WHERE rnk <= 1
ORDER BY id DESC;
