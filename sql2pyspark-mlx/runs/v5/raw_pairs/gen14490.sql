SELECT
    s.delivered_date,
    o.status,
    o.store_id,
    st.store_name,
    st.sqft
FROM shipments s
JOIN orders o ON s.order_id = o.id
LEFT JOIN stores st ON o.store_id = st.id
WHERE YEAR(o.order_date) = 2025 AND o.amount < 1500
ORDER BY s.delivered_date;
