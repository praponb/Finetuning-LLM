WITH src AS (
    SELECT *
    FROM inventory
    WHERE last_restock BETWEEN '2023-11-01' AND '2024-02-09' AND last_restock > '2025-03-04' AND product_id IN (5, 7)
),
inventory_stats AS (
    SELECT product_id, MAX(quantity) - MIN(quantity) AS quantity_range
    FROM src
    GROUP BY product_id
)
SELECT *
FROM inventory_stats
ORDER BY product_id
LIMIT 5;
