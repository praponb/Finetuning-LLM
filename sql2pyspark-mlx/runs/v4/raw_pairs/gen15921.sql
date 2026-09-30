SELECT
    i.id,
    i.store_id,
    p.product_name,
    s.region,
    s.city
FROM inventory i
LEFT OUTER JOIN products p ON i.product_id = p.id
RIGHT OUTER JOIN stores s ON i.store_id = s.id;
