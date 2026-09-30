SELECT id, amount, order_id, DATEDIFF(CURRENT_DATE(), paid_at) AS days_since_paid_at, LENGTH(method) AS method_length FROM payments WHERE paid_at >= '2023-07-01' AND amount < 1370
