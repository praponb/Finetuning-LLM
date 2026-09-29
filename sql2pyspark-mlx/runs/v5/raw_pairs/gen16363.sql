SELECT amount, LENGTH(method) AS method_length
FROM payments
WHERE paid_at >= '2024-03-27' AND TO_DATE(paid_at) >= '2024-03-01'
