SELECT amount
FROM payments
INTERSECT
SELECT cost
FROM products;
