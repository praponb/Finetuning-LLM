SELECT
    id,
    order_id,
    status,
    amount,
    CASE WHEN method = 'promptpay' THEN 1 ELSE 0 END is_promptpay,
    CASE WHEN method = 'card' THEN 1 ELSE 0 END is_card
FROM payments
WHERE NOT (method IN ('promptpay', 'bank_transfer', 'card') AND method = 'bank_transfer')
ORDER BY is_promptpay DESC
