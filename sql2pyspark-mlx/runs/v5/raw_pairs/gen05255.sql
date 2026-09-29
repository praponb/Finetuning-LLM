SELECT id, amount, order_id, LENGTH(method) AS method_length FROM payments WHERE paid_at >= '2023-10-01' ORDER BY id DESC
