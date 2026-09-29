SELECT orders.channel, SUM(payments.amount) AS total_amount
FROM payments
RIGHT JOIN orders ON payments.order_id = orders.id
INNER JOIN customers ON orders.customer_id = customers.id
GROUP BY orders.channel
HAVING SUM(payments.amount) >= 7200
ORDER BY total_amount DESC;
