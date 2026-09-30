SELECT
    i.id,
    i.store_id,
    p.launch_date,
    p.brand,
    p.product_name,
    DATE_FORMAT(i.last_restock, 'yyyy-MM-dd') AS last_restock_label
FROM inventory i
JOIN products p ON p.id = i.product_id
ORDER BY last_restock_label DESC, launch_date ASC
