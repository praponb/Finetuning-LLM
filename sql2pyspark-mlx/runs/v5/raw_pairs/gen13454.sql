SELECT
    o.id,
    o.order_date,
    o.discount,
    s.city,
    s.sqft,
    c.credit_limit,
    c.segment,
    CONCAT_WS('/', c.name, c.city) AS name_city_key
FROM orders o
JOIN stores s ON o.store_id = s.id
JOIN customers c ON c.id = o.customer_id
WHERE o.discount > 0.11 AND LENGTH(c.name) > 8
ORDER BY s.sqft DESC, s.city;
