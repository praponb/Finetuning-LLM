SELECT *, DATE_FORMAT(last_restock, 'yyyy-MM') AS last_restock_label, CAST(quantity AS STRING) AS quantity_num
FROM inventory
WHERE product_id IN (6, 10) AND store_id IN (1, 3)
ORDER BY quantity DESC, last_restock_label DESC
