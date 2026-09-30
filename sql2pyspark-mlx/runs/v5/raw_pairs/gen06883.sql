SELECT shipped_date, 'shipment' AS source
FROM shipments
WHERE order_id IS NOT NULL
UNION ALL
SELECT opened_date, 'account' AS source
FROM accounts
ORDER BY source;
