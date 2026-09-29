SELECT
    s.id,
    s.delivered_date,
    s.cost,
    o.discount,
    o.customer_id,
    st.store_name
FROM shipments s
JOIN orders o ON o.id = s.order_id
LEFT JOIN stores st ON o.store_id = st.id
WHERE s.cost > 87
ORDER BY s.delivered_date ASC;
