SELECT product_id, last_restock_quarter, min_quantity
FROM (
    SELECT product_id, QUARTER(last_restock) AS last_restock_quarter, MIN(quantity) AS min_quantity
    FROM inventory
    WHERE last_restock < '2023-05-01'
    GROUP BY product_id, QUARTER(last_restock)
) AS s
WHERE min_quantity > 140
ORDER BY product_id, last_restock_quarter
LIMIT 50
