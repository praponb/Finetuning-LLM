SELECT
    id,
    shipped_date,
    order_id,
    status
FROM shipments
WHERE shipped_date = (SELECT MIN(shipped_date) FROM shipments);
