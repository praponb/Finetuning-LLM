SELECT
    s.id,
    s.delivered_date,
    o.customer_id,
    st.store_name,
    st.opened_date
FROM shipments s
JOIN orders o ON o.id = s.order_id
JOIN stores st ON o.store_id = st.id
WHERE o.status != 'RETURNED' AND s.status IN ('RETURNED', 'DELIVERED');
