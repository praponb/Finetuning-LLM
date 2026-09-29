SELECT amount, ROUND(amount, 1) AS amount_rounded FROM payments ORDER BY amount_rounded, amount DESC
