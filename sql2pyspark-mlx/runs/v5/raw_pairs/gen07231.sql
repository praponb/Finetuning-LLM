SELECT order_id, bank_transfer_amount
FROM (
    SELECT order_id, SUM(CASE WHEN method = 'bank_transfer' THEN amount ELSE 0 END) AS bank_transfer_amount
    FROM payments
    GROUP BY order_id
) AS sub
WHERE bank_transfer_amount >= 42
ORDER BY order_id DESC;
