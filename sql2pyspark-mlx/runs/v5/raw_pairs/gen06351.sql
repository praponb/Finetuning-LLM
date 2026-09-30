WITH filtered_inventory AS (
    SELECT *, ROUND(quantity * 1.15, 2) AS quantity_adjusted
    FROM inventory
    WHERE last_restock BETWEEN '2024-03-22' AND '2025-05-28' AND (store_id IN (2, 3, 5, 6) OR product_id IN (1, 7))
),
agg AS (
    SELECT store_id, CASE WHEN quantity_adjusted >= 67 THEN 'high' WHEN quantity_adjusted >= 15 THEN 'mid' ELSE 'low' END AS quantity_adjusted_bucket, COUNT(DISTINCT quantity) AS unique_stores
    FROM filtered_inventory
    GROUP BY store_id, CASE WHEN quantity_adjusted >= 67 THEN 'high' WHEN quantity_adjusted >= 15 THEN 'mid' ELSE 'low' END
)
SELECT store_id, quantity_adjusted_bucket, unique_stores
FROM agg
ORDER BY unique_stores DESC;
