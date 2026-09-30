SELECT c.city, c.email, t.first_end_date
FROM customers c
JOIN (SELECT customer_id, MIN(end_date) AS first_end_date
    FROM subscriptions
    GROUP BY customer_id) t ON c.id = t.customer_id
ORDER BY city DESC;
