SELECT
    o.id,
    o.discount,
    s.sqft,
    s.store_name,
    s.city
FROM orders o
INNER JOIN stores s ON o.store_id = s.id
WHERE o.channel = 'partner';
