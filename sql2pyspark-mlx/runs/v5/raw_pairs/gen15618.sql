SELECT status, 'account' AS record_type FROM accounts WHERE customer_id IS NOT NULL UNION ALL SELECT status, 'shipment' AS record_type FROM shipments ORDER BY record_type
