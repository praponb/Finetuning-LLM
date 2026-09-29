SELECT merchant, ROUND(amount, 2) AS amount_rounded FROM transactions WHERE amount BETWEEN 1200 AND 3400 ORDER BY amount_rounded;
