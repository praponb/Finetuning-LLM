SELECT status, 'account' AS record_type
FROM accounts
UNION
SELECT status, 'shipment' AS record_type
FROM shipments;
