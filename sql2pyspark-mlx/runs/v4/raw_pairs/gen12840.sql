WITH src AS (
    SELECT *
    FROM customers
    WHERE segment IN ('Corporate', 'SMB') AND country = 'UK' AND city != 'Berlin'
),
country_summary AS (
    SELECT country, ROUND(AVG(credit_limit), 2) avg_credit_limit, ROUND((100 * SUM(CASE WHEN country = 'DE' THEN 1 ELSE 0 END)) / COUNT(*), 1) pct_de
    FROM src
    GROUP BY country
)
SELECT country, pct_de
FROM country_summary
ORDER BY country;
