SELECT MIN(amount) AS min_amount, ROUND(AVG(amount), 2) AS avg_amount FROM transactions WHERE amount BETWEEN 1600 AND 3700;
