SELECT
    s.id,
    s.carrier,
    s.shipped_date,
    o.amount,
    o.order_date,
    st.store_name
FROM shipments s
JOIN orders o ON s.order_id = o.id
JOIN stores st ON o.store_id = st.id
WHERE YEAR(o.order_date) >= 2023 OR o.discount < 0.12;
