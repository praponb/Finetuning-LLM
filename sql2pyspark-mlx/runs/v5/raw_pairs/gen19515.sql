SELECT
    o.id,
    o.shipping_fee,
    c.city,
    c.credit_limit,
    s.opened_date,
    s.region
FROM orders o
INNER JOIN customers c ON o.customer_id = c.id
LEFT JOIN stores s ON o.store_id = s.id
WHERE s.city IN ('Tokyo', 'London')
