SELECT o.status, ROUND((100 * SUM(CASE WHEN s.carrier = 'DHL' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_dhl
FROM shipments s
INNER JOIN orders o ON s.order_id = o.id
LEFT OUTER JOIN stores st ON o.store_id = st.id
WHERE o.customer_id IS NOT NULL
GROUP BY o.status;
