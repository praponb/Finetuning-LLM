SELECT *
FROM payments
WHERE method NOT IN ('promptpay', 'bank_transfer', 'card') AND order_id IS NULL
