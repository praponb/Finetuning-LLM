SELECT paid_at, amount, method, COALESCE(amount, 0) AS amount_filled FROM payments;
