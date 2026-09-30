SELECT
    s.id,
    s.order_id,
    o.shipping_fee,
    st.store_name
FROM shipments s
RIGHT JOIN orders o ON s.order_id = o.id
LEFT JOIN stores st ON st.id = o.store_id
