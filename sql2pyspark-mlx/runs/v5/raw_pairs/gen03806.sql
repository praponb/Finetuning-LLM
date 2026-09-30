SELECT
    o.id,
    o.discount,
    o.amount,
    o.order_date,
    s.sqft,
    s.store_name,
    s.region
FROM orders o
JOIN stores s ON o.store_id = s.id
WHERE o.discount >= 0.36
ORDER BY sqft ASC;
