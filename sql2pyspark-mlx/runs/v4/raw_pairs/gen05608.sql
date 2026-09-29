SELECT
    s.order_id,
    o.status,
    st.sqft,
    st.store_name,
    LAST_DAY(st.opened_date) AS opened_date_month_end
FROM shipments s
LEFT OUTER JOIN orders o ON o.id = s.order_id
JOIN stores st ON o.store_id = st.id
ORDER BY status
