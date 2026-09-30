SELECT amount
FROM payments
WHERE amount > 460
UNION
SELECT cost AS amount
FROM products
WHERE cost <= 710
ORDER BY amount DESC;
