SELECT
    s.id,
    s.status,
    s.delivered_date,
    o.order_date,
    st.store_name
FROM shipments s
INNER JOIN orders o ON s.order_id = o.id
INNER JOIN stores st ON o.store_id = st.id
ORDER BY id
