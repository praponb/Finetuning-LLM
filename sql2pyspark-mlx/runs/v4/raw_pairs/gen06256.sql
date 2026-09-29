SELECT
    s.id,
    s.carrier,
    s.delivered_date,
    o.store_id,
    st.sqft,
    st.store_name
FROM shipments s
LEFT OUTER JOIN orders o ON o.id = s.order_id
LEFT OUTER JOIN stores st ON o.store_id = st.id
WHERE st.store_name LIKE 'Mal%' AND st.region NOT IN ('East', 'West');
