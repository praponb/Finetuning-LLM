SELECT
    o.store_id,
    o.shipping_fee,
    o.order_date,
    c.is_active,
    c.name,
    c.age
FROM orders o
INNER JOIN customers c ON o.customer_id = c.id
WHERE c.country NOT IN ('UK', 'JP', 'TH')
ORDER BY shipping_fee DESC
