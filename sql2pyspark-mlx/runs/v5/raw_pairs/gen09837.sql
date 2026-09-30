WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, DATE_SUB(last_restock, 28) last_restock_minus_28d
    FROM inventory
    WHERE quantity != 67
)
SELECT id, product_id
FROM filtered
INTERSECT ALL
SELECT id, product_id
FROM inventory
WHERE last_restock BETWEEN '2023-07-06' AND '2024-07-01';
