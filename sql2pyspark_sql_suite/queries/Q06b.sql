-- Q06b: Semi/anti joins and correlated subqueries
-- needs setup views: customers, orders
-- @id: Q06b
SELECT c.customer_id, c.full_name, c.signup_date, size(map_keys(c.preferences)) AS n_prefs
FROM customers c
LEFT ANTI JOIN orders o ON o.customer_id = c.customer_id
WHERE c.signup_date < DATE'2024-01-01';
