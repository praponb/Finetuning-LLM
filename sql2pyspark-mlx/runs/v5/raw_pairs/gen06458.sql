SELECT id, method, amount, LENGTH(method) AS method_length, DATE_FORMAT(paid_at, 'yyyy-MM') AS paid_at_label FROM payments
