SELECT c.country, COUNT(o.shipping_fee) AS shipping_fee_count
FROM orders o
INNER JOIN customers c ON o.customer_id = c.id
GROUP BY c.country
HAVING shipping_fee_count > 4;
