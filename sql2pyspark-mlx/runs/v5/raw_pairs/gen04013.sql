SELECT
    QUARTER(c.signup_date) AS signup_quarter,
    MONTH(c.signup_date) AS signup_month,
    CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(s.country))) AS country_values,
    COUNT(*) AS row_count
FROM sessions s
INNER JOIN customers c ON s.customer_id = c.id
WHERE NOT c.is_active
GROUP BY QUARTER(c.signup_date), MONTH(c.signup_date)
ORDER BY signup_month DESC;
