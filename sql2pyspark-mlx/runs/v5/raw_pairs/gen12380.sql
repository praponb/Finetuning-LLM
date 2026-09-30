SELECT MIN(amount) min_amount
FROM payments
WHERE method = 'cash';
