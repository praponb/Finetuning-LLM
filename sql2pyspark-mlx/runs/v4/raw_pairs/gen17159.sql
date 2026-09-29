SELECT
    o.shipping_fee,
    c.segment,
    s.region,
    s.city
FROM orders o
INNER JOIN customers c ON c.id = o.customer_id
INNER JOIN stores s ON s.id = o.store_id;
