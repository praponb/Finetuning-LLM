WITH valid AS (
    SELECT
        id,
        full_name,
        email,
        dept_id,
        manager_id,
        job_title,
        hire_date,
        DAYOFWEEK(hire_date) AS hire_date_dayofweek,
        CASE WHEN salary >= 143000 THEN 'High' WHEN salary >= 54000 THEN 'Medium' ELSE 'Low' END AS salary_band
    FROM employees
    WHERE hire_date > DATE '2023-03-16'
)
SELECT
    a.*,
    p.level,
    p.full_name AS employee_full_name,
    p.job_title AS employee_job_title
FROM valid a
LEFT JOIN employees p
    ON a.manager_id = p.id;
