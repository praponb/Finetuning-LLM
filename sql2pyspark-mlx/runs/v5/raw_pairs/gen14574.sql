SELECT
    s.id,
    s.order_id,
    o.status,
    st.region
FROM shipments s
INNER JOIN orders o ON s.order_id = o.id
LEFT JOIN stores st ON o.store_id = st.id
WHERE st.region IN ('West', 'Central', 'North')
ORDER BY order_id DESC, id
LIMIT 10
