SELECT id, order_id, DATE_FORMAT(shipped_date, 'MMM yyyy') AS shipped_date_label
FROM shipments
WHERE delivered_date <= '2025-01-01' OR status != 'DELIVERED';
