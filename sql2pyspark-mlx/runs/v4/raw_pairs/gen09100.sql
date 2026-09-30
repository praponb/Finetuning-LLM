WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CONCAT_WS('/', level, full_name) AS level_full_name_key,
        CONCAT(job_title, ' ', level) AS job_title_level
    FROM employees
    WHERE hire_date >= TO_DATE('2023-03-03')
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        full_name,
        email,
        dept_id,
        manager_id,
        job_title,
        salary,
        hire_date,
        is_remote,
        level,
        level_full_name_key,
        job_title_level,
        ROUND(salary / SUM(salary) OVER (PARTITION BY dept_id), 4) AS salary_share,
        LAST_VALUE(level, TRUE) OVER (PARTITION BY level ORDER BY hire_date ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_level,
        ROW_NUMBER() OVER w_part AS event_seq
    FROM cleaned
    WINDOW w_part AS (PARTITION BY level ORDER BY salary DESC, id ASC)
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY level ORDER BY salary DESC, id ASC) AS rank_in_group
    FROM scored
    QUALIFY rank_in_group = 1
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM latest
    QUALIFY ROW_NUMBER() OVER (PARTITION BY dept_id, job_title ORDER BY salary DESC, id ASC) = 1
),
ranked_employees AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY job_title, dept_id ORDER BY salary ASC, id ASC) AS dedup_rank
        FROM ranked
    ) t
    WHERE dedup_rank = 1
)
SELECT
    s.id,
    s.full_name,
    s.email,
    s.dept_id,
    s.manager_id,
    s.job_title,
    s.hire_date,
    s.is_remote,
    s.last_known_level,
    (SELECT SUM(x.salary) FROM employees x WHERE x.dept_id = s.dept_id) AS sum_employee_salary
FROM ranked_employees s
ORDER BY dept_id ASC NULLS FIRST
