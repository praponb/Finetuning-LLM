SELECT *
FROM shipments
WHERE NOT (order_id IN (8, 26) AND (shipped_date < '2024-03-01' OR shipped_date <= '2024-02-14'))
ORDER BY delivered_date, status DESC;
