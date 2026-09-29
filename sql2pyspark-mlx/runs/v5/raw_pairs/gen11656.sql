WITH cleaned AS (
    SELECT
        id,
        full_name,
        email,
        dept_id,
        manager_id,
        job_title,
        hire_date,
        level,
        LOWER(job_title) job_title_lower
    FROM employees
    WHERE job_title IS NOT NULL
      AND level IS DISTINCT FROM 'L2'
),
with_dims AS (
    SELECT
        a.id,
        a.full_name,
        a.dept_id,
        a.job_title,
        a.hire_date,
        b.dept_name
    FROM cleaned a
    LEFT JOIN departments b
        ON a.dept_id = b.id
    WHERE b.dept_name <> 'Finance'
),
ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY dept_id, dept_name ORDER BY hire_date DESC, id) AS latest_rank
    FROM with_dims
    QUALIFY latest_rank = 1
),
enriched AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY job_title, dept_name ORDER BY latest_rank, id) row_seq
    FROM ranked
),
enriched_employees AS (
    SELECT
        *,
        SUM(row_seq) OVER (PARTITION BY job_title ORDER BY latest_rank, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cumulative_row_seq,
        LEAD(row_seq, 2) OVER (PARTITION BY dept_id ORDER BY latest_rank, id) next_row_seq,
        ROW_NUMBER() OVER (ORDER BY latest_rank, id) row_seq_2,
        LAST_VALUE(job_title, TRUE) OVER (PARTITION BY dept_name ORDER BY latest_rank, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) last_known_job_title
    FROM enriched
),
with_windows AS (
    SELECT
        id,
        dept_id,
        job_title,
        row_seq,
        row_seq_2,
        last_known_job_title,
        SUM(row_seq) OVER (PARTITION BY dept_name, job_title ORDER BY row_seq, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cumulative_row_seq_2,
        AVG(next_row_seq) OVER (ORDER BY row_seq_2, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) next_row_seq_avg_3
    FROM enriched_employees
),
by_type AS (
    SELECT *
    FROM (
        SELECT
            dept_id,
            job_title,
            row_seq_2
        FROM with_windows
    ) src
    PIVOT (
        MAX(row_seq_2)
        FOR job_title IN ('Manager' AS manager, 'Analyst' AS analyst)
    )
)
SELECT
    s.dept_id,
    s.analyst,
    (SELECT MAX(e2.salary) FROM employees e2 WHERE e2.dept_id = s.dept_id) AS max_employee_salary
FROM by_type s
ORDER BY analyst DESC NULLS FIRST;
