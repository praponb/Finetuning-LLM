SELECT
    s.status,
    s.order_id,
    o.discount,
    o.status AS order_status,
    st.store_name,
    st.opened_date
FROM shipments s
RIGHT JOIN orders o ON s.order_id = o.id
INNER JOIN stores st ON o.store_id = st.id
WHERE s.shipped_date BETWEEN '2024-11-12' AND '2025-01-24'
ORDER BY order_status, store_name DESC
