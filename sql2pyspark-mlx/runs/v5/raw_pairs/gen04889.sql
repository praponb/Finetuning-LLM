SELECT status FROM payments WHERE method IN ('card', 'paypal') UNION ALL SELECT status FROM shipments;
