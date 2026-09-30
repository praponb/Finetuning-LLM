SELECT unit_price, 'order_item' AS record_type
FROM order_items
UNION ALL
SELECT amount AS unit_price, 'transaction' AS record_type
FROM transactions
WHERE account_id IN (5, 37)
ORDER BY record_type;
