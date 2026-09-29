SELECT shipping_fee
FROM orders
EXCEPT
SELECT amount AS shipping_fee
FROM payments
ORDER BY shipping_fee;
