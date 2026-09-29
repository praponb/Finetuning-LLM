WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        SPLIT(LOWER(email), '@')[0] AS email_user,
        COALESCE(level, 'DIRECT') AS resolved_level
    FROM employees
    WHERE hire_date >= TO_DATE('2023-05-21')
      AND job_title IS NOT DISTINCT FROM 'Designer'
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY job_title ORDER BY hire_date NULLS FIRST, id) AS rank_in_group
        FROM cleaned
    ) x
    WHERE rank_in_group = 1
),
cleaned_employees AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        full_name,
        dept_id,
        manager_id,
        job_title,
        hire_date,
        level,
        email_user,
        resolved_level,
        rank_in_group,
        CAST(is_remote AS INT) AS is_remote_flag,
        rank_in_group + salary AS rank_in_group_plus_salary,
        COALESCE(level, 'DIRECT') AS level_filled
    FROM deduped
    WHERE job_title IS DISTINCT FROM 'Designer'
),
joined AS (
    -- Attach employees attributes
    SELECT
        a.*,
        p.level AS employee_level,
        p.hire_date AS employee_hire_date
    FROM cleaned_employees a
    LEFT JOIN employees p
        ON a.manager_id = p.id
)
SELECT
    s.id,
    s.full_name,
    s.dept_id,
    s.level,
    s.email_user,
    s.resolved_level,
    s.rank_in_group,
    s.is_remote_flag,
    s.rank_in_group_plus_salary,
    (SELECT MAX(x.salary) FROM employees x WHERE x.dept_id = s.dept_id) AS max_employee_salary
FROM joined s
