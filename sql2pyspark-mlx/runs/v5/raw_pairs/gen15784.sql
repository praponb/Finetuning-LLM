WITH cleaned AS (
    SELECT *, REGEXP_REPLACE(full_name, ' ', '') AS full_name_slug
    FROM employees
    WHERE hire_date > DATE '2023-08-04'
),
with_windows AS (
    SELECT
        id,
        full_name,
        email,
        dept_id,
        manager_id,
        job_title,
        level,
        NTILE(5) OVER (ORDER BY salary DESC, id) AS salary_bucket,
        ROW_NUMBER() OVER w_grp AS row_seq
    FROM cleaned
    WINDOW w_grp AS (PARTITION BY job_title, dept_id ORDER BY hire_date DESC, id)
),
filtered AS (
    SELECT
        id,
        dept_id,
        manager_id,
        job_title,
        level,
        salary_bucket,
        row_seq,
        salary_bucket - row_seq AS salary_bucket_minus_row_seq
    FROM with_windows
    WHERE job_title IS NOT NULL
),
metrics AS (
    SELECT
        id,
        dept_id,
        manager_id,
        job_title,
        salary_bucket,
        salary_bucket_minus_row_seq,
        LEAD(salary_bucket, 2) OVER (PARTITION BY dept_id ORDER BY salary_bucket, id) AS next_salary_bucket,
        SUM(salary_bucket_minus_row_seq) OVER (PARTITION BY job_title, level ORDER BY row_seq, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_salary_bucket_minus_row_seq,
        LEAD(salary_bucket_minus_row_seq, 2) OVER (PARTITION BY dept_id ORDER BY salary_bucket_minus_row_seq, id) AS next_salary_bucket_minus_row_seq
    FROM filtered
),
enriched AS (
    SELECT s.*, d.budget
    FROM metrics s
    INNER JOIN departments d
        ON s.dept_id = d.id
),
with_windows_employees AS (
    SELECT
        id,
        dept_id,
        job_title,
        salary_bucket_minus_row_seq,
        budget,
        LEAD(next_salary_bucket_minus_row_seq, 1, 0) OVER w_running AS next_next_salary_bucket_minus_row_seq,
        SUM(salary_bucket) OVER w_part AS cumulative_salary_bucket,
        AVG(salary_bucket_minus_row_seq) OVER (PARTITION BY job_title, manager_id ORDER BY next_salary_bucket_minus_row_seq, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS salary_bucket_minus_row_seq_avg_2,
        ROUND(next_salary_bucket / SUM(next_salary_bucket) OVER (PARTITION BY manager_id), 4) AS next_salary_bucket_share,
        NTILE(4) OVER w_hist AS salary_bucket_minus_row_seq_bucket
    FROM enriched
    WINDOW w_running AS (PARTITION BY dept_id ORDER BY salary_bucket, id), w_part AS (PARTITION BY manager_id ORDER BY next_salary_bucket, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_hist AS (PARTITION BY dept_id ORDER BY salary_bucket_minus_row_seq DESC, id)
),
scored AS (
    SELECT
        *,
        LEAD(salary_bucket_minus_row_seq_bucket) OVER (PARTITION BY dept_id ORDER BY next_next_salary_bucket_minus_row_seq, id) AS next_salary_bucket_minus_row_seq_bucket,
        LAG(salary_bucket_minus_row_seq_bucket) OVER (PARTITION BY dept_id ORDER BY next_next_salary_bucket_minus_row_seq, id) AS prev_salary_bucket_minus_row_seq_bucket,
        LAG(salary_bucket_minus_row_seq_avg_2) OVER (PARTITION BY dept_id ORDER BY next_next_salary_bucket_minus_row_seq, id) AS prev_salary_bucket_minus_row_seq_avg_2,
        ROW_NUMBER() OVER (PARTITION BY dept_id ORDER BY next_next_salary_bucket_minus_row_seq, id) AS row_seq
    FROM with_windows_employees
)
SELECT
    id,
    job_title,
    budget,
    cumulative_salary_bucket,
    salary_bucket_minus_row_seq_avg_2,
    salary_bucket_minus_row_seq_bucket,
    AVG(budget) OVER (PARTITION BY job_title, dept_id ORDER BY salary_bucket_minus_row_seq, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS budget_avg_5,
    STDDEV_SAMP(cumulative_salary_bucket) OVER (PARTITION BY job_title) AS job_title_stddev_samp_cumulative_salary_bucket,
    AVG(next_salary_bucket_minus_row_seq_bucket) OVER (PARTITION BY job_title ORDER BY prev_salary_bucket_minus_row_seq_avg_2, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS next_salary_bucket_minus_row_seq_bucket_avg_2_prior,
    SUM(cumulative_salary_bucket) OVER (PARTITION BY dept_id ORDER BY prev_salary_bucket_minus_row_seq_bucket, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_cumulative_salary_bucket
FROM scored
