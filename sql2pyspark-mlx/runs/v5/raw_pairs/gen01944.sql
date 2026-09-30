SELECT
    s.region,
    o.shipping_fee,
    o.store_id,
    o.channel
FROM stores s
LEFT OUTER JOIN orders o ON o.store_id = s.id
WHERE s.region = 'South';
