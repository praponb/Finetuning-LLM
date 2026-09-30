SELECT
    o.id,
    o.discount,
    s.sqft,
    c.credit_limit,
    c.name
FROM orders o
LEFT JOIN stores s ON o.store_id = s.id
LEFT JOIN customers c ON o.customer_id = c.id
WHERE o.channel <> 'store'
ORDER BY id DESC, name ASC;
