WITH filtered_inventory AS (
    SELECT *
    FROM inventory
    WHERE last_restock BETWEEN '2024-09-01' AND '2024-09-18' AND MONTH(last_restock) IN (3, 5, 8) AND product_id = 4
),
agg AS (
    SELECT product_id, MAX(quantity) - MIN(quantity) AS quantity_range, MAX(last_restock) AS last_last_restock
    FROM filtered_inventory
    GROUP BY product_id
)
SELECT product_id, quantity_range, last_last_restock
FROM agg
WHERE quantity_range > 61
ORDER BY product_id DESC;
