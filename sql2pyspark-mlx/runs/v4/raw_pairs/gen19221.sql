WITH with_dims AS (
    -- Attach employees attributes
    SELECT
        src.*,
        d.email AS employee_email,
        d.level AS employee_level
    FROM employees src
    INNER JOIN employees d
        ON src.manager_id = d.id
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY level ORDER BY hire_date DESC NULLS LAST, id ASC) AS rank_in_group
        FROM with_dims
    ) x
    WHERE rank_in_group = 1
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        *,
        LOWER(employee_email) employee_email_lower,
        LOWER(email) email_lower
    FROM ranked
    WHERE job_title IS NOT NULL
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY job_title ORDER BY salary DESC, id ASC) AS row_num
    FROM cleaned
    QUALIFY row_num = 1
)
-- Aggregate with conditional metrics
SELECT
    employee_level,
    CASE WHEN salary >= 133000 THEN 'HIGH' ELSE 'LOW' END AS salary_tier,
    COUNT(*) FILTER (WHERE employee_level = 'L3') AS l3_rows,
    COUNT(*) AS cnt,
    GROUPING(employee_level) AS grouping_id
FROM deduped
GROUP BY ROLLUP (employee_level, CASE WHEN salary >= 133000 THEN 'HIGH' ELSE 'LOW' END)
HAVING COUNT(*) < 1
