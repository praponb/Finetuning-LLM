SELECT
    ss.id,
    ss.revenue,
    s.store_name,
    s.sqft,
    p.price
FROM store_sales ss
JOIN stores s ON ss.store_id = s.id
JOIN products p ON ss.product_id = p.id;
