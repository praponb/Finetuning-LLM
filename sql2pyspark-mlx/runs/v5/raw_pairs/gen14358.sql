WITH enriched AS (
    -- Attach employees attributes
    SELECT
        s.id,
        s.full_name,
        s.manager_id,
        s.job_title,
        s.hire_date,
        s.is_remote,
        s.level,
        d.level AS employee_level,
        d.email AS employee_email,
        d.salary AS employee_salary
    FROM employees s
    LEFT JOIN employees d
        ON s.manager_id <=> d.id
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY level ORDER BY hire_date DESC NULLS FIRST) AS latest_rank
        FROM enriched
    ) ranked
    WHERE latest_rank = 1
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        full_name,
        manager_id,
        job_title,
        hire_date,
        is_remote,
        employee_level,
        employee_email,
        employee_salary,
        latest_rank,
        ROW_NUMBER() OVER (ORDER BY hire_date, id) AS row_seq,
        COUNT(employee_salary) OVER (PARTITION BY job_title) AS job_title_count_employee_salary,
        ROUND(employee_salary / SUM(employee_salary) OVER (PARTITION BY job_title), 4) AS employee_salary_share
    FROM deduped
    WHERE LOWER(full_name) LIKE '%any%'
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        full_name,
        manager_id,
        job_title,
        employee_level,
        employee_salary_share,
        MIN(employee_salary_share) OVER w_running AS last4_min_employee_salary_share,
        COUNT(employee_salary_share) OVER w_running AS last4_count_employee_salary_share,
        MAX(employee_salary_share) OVER w_running AS last4_max_employee_salary_share
    FROM with_windows
    WINDOW w_running AS (PARTITION BY employee_level ORDER BY employee_salary_share, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW)
),
joined AS (
    -- Attach employees attributes
    SELECT
        a.id,
        a.manager_id,
        a.job_title,
        a.employee_level,
        a.employee_salary_share,
        p.hire_date
    FROM scored a
    LEFT JOIN employees p
        ON a.manager_id = p.id
)
-- Join a dimension and aggregate
SELECT
    p.level,
    e.employee_level,
    COUNT(*) AS num_rows,
    ROUND(SUM(e.employee_salary_share), 2) AS total_employee_salary_share
FROM joined e
LEFT JOIN employees p ON e.manager_id = p.id
GROUP BY p.level, e.employee_level;
