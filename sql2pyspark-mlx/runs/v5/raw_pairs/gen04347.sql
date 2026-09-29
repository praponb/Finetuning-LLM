SELECT
    product_id,
    DATE_FORMAT(last_restock, 'yyyy-MM') AS last_restock_month,
    MIN(quantity) AS min_quantity,
    MAX(last_restock) AS last_last_restock
FROM inventory
GROUP BY 1, 2
ORDER BY min_quantity DESC, product_id, last_restock_month
LIMIT 20;
