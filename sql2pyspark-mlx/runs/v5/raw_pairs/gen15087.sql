SELECT paid_at
FROM payments
WHERE NOT (order_id IS NOT NULL AND method != 'card');
