SELECT o.customer_id, COUNT(CASE WHEN s.carrier = 'Kerry' THEN 1 END) AS kerry_count
FROM shipments s
LEFT JOIN orders o ON s.order_id = o.id
LEFT JOIN stores st ON o.store_id = st.id
WHERE st.store_name LIKE '%Downt%'
GROUP BY o.customer_id
ORDER BY kerry_count DESC, customer_id DESC
