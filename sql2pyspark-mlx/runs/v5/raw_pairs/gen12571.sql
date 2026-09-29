SELECT
    o.id,
    o.channel,
    o.order_date,
    o.store_id
FROM orders o
WHERE o.discount > 0.27 AND o.store_id IN (SELECT id FROM stores WHERE region = 'Central')
