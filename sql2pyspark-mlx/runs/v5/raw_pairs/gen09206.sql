WITH with_windows AS (
    SELECT
        *,
        NTILE(10) OVER (PARTITION BY job_title, level ORDER BY salary DESC, id ASC) salary_bucket,
        ROW_NUMBER() OVER (PARTITION BY manager_id ORDER BY salary DESC, id ASC) seq_no
    FROM employees
    WHERE manager_id = 39
)
SELECT
    s.id,
    s.full_name,
    s.dept_id,
    s.job_title,
    s.salary,
    s.level,
    s.salary_bucket,
    s.seq_no,
    (SELECT SUM(t2.salary) FROM employees t2 WHERE t2.dept_id = s.dept_id) AS sum_employee_salary
FROM with_windows s
ORDER BY seq_no DESC NULLS FIRST;
