SELECT *, CAST(cost AS INT) AS cost_int, CASE WHEN carrier = 'DHL' THEN 1 ELSE 0 END AS is_dhl
FROM shipments
WHERE status IN ('RETURNED', 'LOST', 'DELIVERED')
