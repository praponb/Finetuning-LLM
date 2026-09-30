WITH metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        CUME_DIST() OVER (PARTITION BY level ORDER BY salary) AS salary_cume_dist,
        ROW_NUMBER() OVER (PARTITION BY dept_id ORDER BY hire_date DESC, id) AS event_seq,
        ROW_NUMBER() OVER (PARTITION BY dept_id, level ORDER BY salary DESC, id) AS row_seq,
        RANK() OVER (PARTITION BY manager_id, job_title ORDER BY salary DESC) AS salary_rank
    FROM employees
),
enriched AS (
    -- Attach employees attributes
    SELECT a.*, e.level AS employee_level
    FROM metrics a
    LEFT JOIN employees e
        ON a.manager_id = e.id
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY level ORDER BY salary DESC, id) AS latest_rank
    FROM enriched
    QUALIFY latest_rank = 1
),
valid AS (
    -- Keep recent, valid rows
    SELECT
        *,
        INITCAP(TRIM(job_title)) AS job_title_title,
        TRIM(email) AS clean_email
    FROM deduped
    WHERE hire_date >= DATE '2023-09-28'
      AND is_remote
),
deduped_employees AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY employee_level ORDER BY latest_rank, id) AS latest_rank_2
    FROM valid
    QUALIFY latest_rank_2 = 1
),
enriched_employees AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(event_seq / SUM(event_seq) OVER (PARTITION BY manager_id), 4) AS event_seq_share,
        ROUND(salary / SUM(salary) OVER (PARTITION BY job_title), 4) AS salary_share,
        NTILE(10) OVER (PARTITION BY level ORDER BY salary_rank DESC, id) AS salary_rank_bucket
    FROM deduped_employees
)
SELECT
    s.id,
    s.full_name,
    s.email,
    s.manager_id,
    s.job_title,
    s.is_remote,
    s.salary_cume_dist,
    s.event_seq,
    s.row_seq,
    (SELECT SUM(t2.salary) FROM employees t2 WHERE t2.manager_id = s.manager_id) AS sum_employee_salary
FROM enriched_employees s
ORDER BY row_seq DESC;
