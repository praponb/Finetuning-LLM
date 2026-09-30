SELECT
    hire_quarter,
    salary_bucket,
    stddev_salary,
    analyst_count
FROM (
    SELECT
        QUARTER(hire_date) hire_quarter,
        CASE WHEN salary >= 146000 THEN 'high' WHEN salary >= 54000 THEN 'mid' ELSE 'low' END salary_bucket,
        ROUND(STDDEV(salary), 2) stddev_salary,
        COUNT(CASE WHEN job_title = 'Analyst' THEN 1 END) analyst_count
    FROM employees
    WHERE job_title IS NOT NULL
    GROUP BY QUARTER(hire_date), CASE WHEN salary >= 146000 THEN 'high' WHEN salary >= 54000 THEN 'mid' ELSE 'low' END
) s
WHERE stddev_salary > 29
ORDER BY salary_bucket
