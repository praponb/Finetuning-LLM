SELECT
    segment,
    ROUND(AVG(credit_limit), 2) AS avg_credit_limit,
    SUM(CASE WHEN segment = 'Corporate' THEN age ELSE 0 END) AS corporate_age,
    MAX(age) - MIN(age) AS age_range
FROM customers
GROUP BY segment
HAVING SUM(CASE WHEN segment = 'Corporate' THEN age ELSE 0 END) > 69
ORDER BY corporate_age DESC
