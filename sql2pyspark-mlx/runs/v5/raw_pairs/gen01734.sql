SELECT cost, 'shipment' kind
FROM shipments
UNION ALL
SELECT amount cost, 'transaction' kind
FROM transactions
WHERE is_flagged
