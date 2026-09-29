SELECT
    status,
    order_id,
    ROUND((100 * SUM(CASE WHEN carrier = 'FedEx' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_fedex,
    SUM(CASE WHEN status = 'IN_TRANSIT' THEN cost ELSE 0 END) AS in_transit_cost
FROM shipments
WHERE shipped_date > '2024-08-01' AND delivered_date < '2024-03-01'
GROUP BY status, order_id
HAVING pct_fedex >= 52;
