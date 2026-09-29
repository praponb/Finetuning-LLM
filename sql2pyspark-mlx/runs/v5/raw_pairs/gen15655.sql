WITH sessions_by_customer AS (
    SELECT customer_id, SUM(CASE WHEN browser = 'Chrome' THEN 1 ELSE 0 END) AS chrome_count
    FROM sessions
    GROUP BY customer_id
)
SELECT
    customers.id,
    customers.name,
    customers.email,
    sessions_by_customer.chrome_count
FROM customers
INNER JOIN sessions_by_customer ON customers.id = sessions_by_customer.customer_id
WHERE sessions_by_customer.chrome_count > 63;
