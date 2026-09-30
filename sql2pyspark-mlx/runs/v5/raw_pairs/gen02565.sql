WITH with_windows AS (
    SELECT
        id,
        full_name,
        dept_id,
        job_title,
        salary,
        hire_date,
        LAG(salary) OVER w_grp AS prev_salary,
        LEAD(salary) OVER w_grp AS next_salary,
        ROW_NUMBER() OVER w_grp AS row_seq,
        NTILE(5) OVER (ORDER BY salary, id) AS salary_bucket
    FROM employees
    WINDOW w_grp AS (PARTITION BY manager_id ORDER BY salary, id)
),
filtered AS (
    SELECT
        id,
        dept_id,
        salary,
        hire_date,
        next_salary,
        row_seq,
        salary_bucket - next_salary AS salary_bucket_minus_next_salary,
        salary_bucket + salary AS salary_bucket_plus_salary,
        COALESCE(job_title, 'OTHER') AS job_title_filled
    FROM with_windows
    WHERE prev_salary < 138000
),
ranked AS (
    SELECT *
    FROM filtered
    QUALIFY ROW_NUMBER() OVER (PARTITION BY dept_id ORDER BY salary_bucket_minus_next_salary DESC NULLS FIRST, id) = 1
),
metrics AS (
    SELECT
        *,
        LAG(salary) OVER (PARTITION BY dept_id ORDER BY salary_bucket_plus_salary, id) AS prev_salary,
        LEAD(row_seq) OVER (PARTITION BY dept_id ORDER BY salary_bucket_plus_salary, id) AS next_row_seq,
        ROW_NUMBER() OVER (PARTITION BY dept_id ORDER BY salary_bucket_plus_salary, id) AS seq_no
    FROM ranked
),
filtered_employees AS (
    SELECT *, DATEDIFF(CURRENT_DATE(), hire_date) AS days_since_hire_date
    FROM metrics
    WHERE seq_no < 6
)
SELECT
    id,
    dept_id,
    salary,
    hire_date,
    next_salary,
    row_seq,
    salary_bucket_minus_next_salary,
    prev_salary,
    seq_no
FROM filtered_employees
ORDER BY hire_date DESC;
