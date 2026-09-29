SELECT
    job_title,
    DATE_TRUNC('MONTH', hire_date) AS hire_month,
    ROUND((100 * SUM(CASE WHEN level = 'L4' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_l4,
    MAX(hire_date) AS latest_hire_date
FROM employees
WHERE full_name LIKE '%Taylo%' AND level IN ('L5', 'L1', 'L3')
GROUP BY 1, 2
ORDER BY pct_l4 DESC, job_title ASC, hire_month ASC
LIMIT 20
