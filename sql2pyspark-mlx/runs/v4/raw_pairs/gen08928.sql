WITH valid_customers AS (
    SELECT *
    FROM customers
    WHERE NOT (city = 'New York' OR is_active)
),
summary AS (
    SELECT city, AVG(credit_limit) AS avg_credit_limit, MAX(credit_limit) AS max_credit_limit
    FROM valid_customers
    GROUP BY city
)
SELECT *
FROM summary;
