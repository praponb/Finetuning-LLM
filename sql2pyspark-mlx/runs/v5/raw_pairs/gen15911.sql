SELECT
    s.carrier,
    o.channel,
    o.status,
    st.store_name,
    st.opened_date
FROM shipments s
INNER JOIN orders o ON s.order_id = o.id
INNER JOIN stores st ON o.store_id = st.id
WHERE st.sqft > 6420
ORDER BY status
