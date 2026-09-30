SELECT
    o.id,
    o.channel,
    o.order_date,
    o.amount,
    s.sqft,
    s.city
FROM orders o
JOIN stores s ON o.store_id = s.id
