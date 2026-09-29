SELECT
    s.sqft,
    s.city,
    s.opened_date,
    o.shipping_fee,
    o.order_date,
    o.channel,
    DATE_TRUNC('WEEK', o.order_date) AS order_date_period
FROM stores s
LEFT JOIN orders o ON o.store_id = s.id;
