SELECT
    s.sqft,
    o.discount,
    o.shipping_fee,
    o.customer_id
FROM stores s
LEFT OUTER JOIN orders o ON s.id = o.store_id
WHERE MONTH(s.opened_date) IN (8, 9, 10) AND o.channel IN ('mobile', 'store', 'partner')
