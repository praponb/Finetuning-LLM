SELECT id, carrier
FROM shipments
WHERE status NOT IN ('RETURNED', 'DELIVERED', 'IN_TRANSIT')
UNION
SELECT id, carrier
FROM shipments
WHERE shipped_date <= '2023-05-01';
