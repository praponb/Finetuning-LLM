SELECT
    s.sqft,
    s.region,
    s.store_name,
    i.quantity
FROM stores s
LEFT JOIN inventory i ON s.id = i.store_id;
