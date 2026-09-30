SELECT
    o.id,
    o.customer_id,
    o.order_date,
    o.status,
    s.sqft,
    s.city
FROM orders o
INNER JOIN stores s ON o.store_id = s.id
WHERE s.region IS NOT NULL;
