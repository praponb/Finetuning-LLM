WITH filtered_customers AS (
    SELECT *
    FROM customers
    WHERE country NOT IN ('UK', 'JP')
),
agg AS (
    SELECT city, country, ROUND(AVG(credit_limit), 2) AS avg_credit_limit
    FROM filtered_customers
    GROUP BY city, country
)
SELECT city, country, avg_credit_limit
FROM agg
WHERE avg_credit_limit > 41
ORDER BY country, city
LIMIT 20
