SELECT method, SUM(CASE WHEN method = 'bank_transfer' THEN 1 ELSE 0 END) AS bank_transfer_count, MIN(amount) AS min_amount
FROM payments
WHERE order_id IS NULL AND order_id IS NOT NULL
GROUP BY method
HAVING min_amount >= 510
