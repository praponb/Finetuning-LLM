SELECT MIN(amount) AS min_amount
FROM orders
WHERE shipping_fee < 25;
