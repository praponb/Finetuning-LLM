SELECT
    o.id,
    o.store_id,
    o.customer_id,
    o.discount,
    s.sqft,
    s.store_name
FROM orders o
INNER JOIN stores s ON o.store_id = s.id
WHERE o.discount < 0.35 AND s.region IN ('East', 'Central', 'South');
