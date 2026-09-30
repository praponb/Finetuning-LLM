SELECT
    s.order_id,
    o.shipping_fee,
    o.amount,
    st.city
FROM shipments s
INNER JOIN orders o ON s.order_id = o.id
LEFT JOIN stores st ON st.id = o.store_id;
