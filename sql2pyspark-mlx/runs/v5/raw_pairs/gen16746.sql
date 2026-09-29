SELECT paid_at, method, amount, LOWER(status) AS status_lower FROM payments WHERE amount < 510
