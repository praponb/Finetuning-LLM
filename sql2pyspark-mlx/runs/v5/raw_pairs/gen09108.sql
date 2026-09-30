SELECT
    id,
    product_id,
    last_restock,
    quantity,
    ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY last_restock, id) row_num,
    quantity - LAG(quantity, 1) OVER (PARTITION BY product_id ORDER BY last_restock, id) quantity_change
FROM inventory
WHERE last_restock BETWEEN '2023-05-27' AND '2024-05-27'
ORDER BY quantity DESC;
