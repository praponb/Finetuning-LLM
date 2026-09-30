SELECT
    s.id,
    s.shipped_date,
    s.delivered_date,
    o.shipping_fee,
    st.region,
    st.opened_date
FROM shipments s
JOIN orders o ON s.order_id = o.id
JOIN stores st ON o.store_id = st.id
ORDER BY opened_date DESC, region, id
LIMIT 50
