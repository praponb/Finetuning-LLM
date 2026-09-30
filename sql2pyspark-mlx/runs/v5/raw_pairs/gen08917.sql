SELECT status, 'payment' origin FROM payments WHERE HOUR(paid_at) BETWEEN 8 AND 11 UNION ALL SELECT status, 'shipment' origin FROM shipments;
