SELECT *, ROUND(amount, 0) AS amount_rounded
FROM payments
WHERE order_id IN (3, 9, 11, 40);
