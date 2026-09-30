SELECT id, amount, TO_DATE(paid_at) AS paid_at_date FROM payments WHERE TO_DATE(paid_at) >= '2024-10-01' OR method NOT IN ('card', 'promptpay', 'bank_transfer')
