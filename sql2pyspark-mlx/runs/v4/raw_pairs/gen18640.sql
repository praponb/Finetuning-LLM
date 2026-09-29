SELECT amount
FROM payments
WHERE method IN ('bank_transfer', 'promptpay')
UNION
SELECT revenue
FROM store_sales
