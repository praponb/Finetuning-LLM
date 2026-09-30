WITH StoreTotals AS (
    SELECT store_id, COUNT(quantity) AS quantity_count, MAX(quantity) AS max_quantity
    FROM inventory
    GROUP BY store_id
)
SELECT
    s.id,
    s.city,
    S.quantity_count,
    S.max_quantity
FROM stores s
INNER JOIN StoreTotals S ON s.id = S.store_id
ORDER BY 2 DESC, id
LIMIT 10
