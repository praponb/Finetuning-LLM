SELECT
    s.sqft,
    s.opened_date,
    s.store_name,
    i.product_id,
    i.store_id
FROM stores s
LEFT JOIN inventory i ON s.id = i.store_id
ORDER BY s.store_name;
