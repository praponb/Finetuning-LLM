SELECT
    ss.revenue,
    ss.store_id,
    s.sqft,
    p.launch_date
FROM store_sales ss
JOIN stores s ON ss.store_id = s.id
JOIN products p ON ss.product_id = p.id
WHERE s.sqft < 12254;
