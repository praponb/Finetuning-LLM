SELECT
    DATE_TRUNC('QUARTER', signup_date) AS quarter_start,
    DATE_TRUNC('MONTH', signup_date) AS activity_month,
    COUNT(CASE WHEN country = 'DE' THEN 1 END) AS de_count,
    SUM(CASE WHEN segment = 'Consumer' THEN age ELSE 0 END) AS consumer_age,
    PERCENTILE_APPROX(age, ARRAY(0.25, 0.5, 0.75)) AS age_quartiles
FROM customers
GROUP BY DATE_TRUNC('QUARTER', signup_date), DATE_TRUNC('MONTH', signup_date)
ORDER BY quarter_start;
