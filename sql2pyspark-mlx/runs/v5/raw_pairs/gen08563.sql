SELECT shipped_date, carrier
FROM shipments
WHERE delivered_date <= '2023-11-01'
UNION
SELECT shipped_date, carrier
FROM shipments
WHERE status <> 'LOST'
ORDER BY shipped_date
