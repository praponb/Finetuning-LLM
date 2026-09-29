SELECT
    s.id,
    s.order_id,
    s.shipped_date,
    o.channel,
    st.sqft
FROM shipments s
INNER JOIN orders o ON s.order_id = o.id
INNER JOIN stores st ON o.store_id = st.id
WHERE YEAR(s.shipped_date) >= 2023;
