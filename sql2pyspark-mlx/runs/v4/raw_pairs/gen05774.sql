SELECT QUARTER(customers.signup_date) signup_quarter, MIN(shipments.cost) min_cost
FROM shipments
INNER JOIN orders ON shipments.order_id = orders.id
LEFT JOIN customers ON orders.customer_id = customers.id
WHERE customers.age BETWEEN 49 AND 55
GROUP BY QUARTER(customers.signup_date)
HAVING min_cost >= 74
ORDER BY min_cost;
