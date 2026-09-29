SELECT *
FROM shipments
WHERE cost BETWEEN 66 AND 96 AND (order_id IS NULL OR status IN ('LOST', 'DELIVERED'));
