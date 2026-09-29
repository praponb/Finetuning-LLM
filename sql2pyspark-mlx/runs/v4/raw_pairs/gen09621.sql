WITH enriched AS (
    SELECT
        *,
        LAST_VALUE(job_title) IGNORE NULLS OVER (PARTITION BY level ORDER BY salary, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_job_title,
        SUM(salary) OVER w_grp AS running_salary
    FROM employees
    WINDOW w_grp AS (PARTITION BY job_title, dept_id ORDER BY hire_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
)
SELECT
    CASE WHEN salary >= 120000 THEN 'LARGE' ELSE 'SMALL' END AS salary_tier,
    DATE_TRUNC('QUARTER', hire_date) AS quarter_start,
    CASE WHEN running_salary >= 620647 THEN 'LARGE' ELSE 'SMALL' END AS running_salary_tier,
    SORT_ARRAY(COLLECT_SET(job_title)) AS job_title_list,
    ROUND(SUM(running_salary) / NULLIF(COUNT(CASE WHEN job_title = 'Intern' THEN 1 END), 0), 2) AS running_salary_per_intern,
    SORT_ARRAY(COLLECT_SET(job_title)) AS job_title_list_2
FROM enriched
GROUP BY CUBE (CASE WHEN salary >= 120000 THEN 'LARGE' ELSE 'SMALL' END, DATE_TRUNC('QUARTER', hire_date), CASE WHEN running_salary >= 620647 THEN 'LARGE' ELSE 'SMALL' END)
ORDER BY quarter_start
