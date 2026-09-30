SELECT
    i.product_id,
    i.last_restock,
    p.brand,
    s.city,
    DATE_TRUNC('month', p.launch_date) AS launch_date_period
FROM inventory i
JOIN products p ON p.id = i.product_id
JOIN stores s ON i.store_id = s.id;
