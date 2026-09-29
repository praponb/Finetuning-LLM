WITH with_last AS (
    -- Latest matching employee per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.dept_name,
        s.budget,
        latest.employee_salary,
        latest.level
    FROM departments s
    JOIN LATERAL (
            SELECT c.salary AS employee_salary, c.level
            FROM employees c
            WHERE c.dept_id = s.id
            ORDER BY c.hire_date DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM with_last s
    WHERE NOT EXISTS (
            SELECT 1
            FROM employees x
            WHERE x.dept_id = s.id
              AND x.email IS NOT NULL
        )
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT *, CEIL(employee_salary) AS employee_salary_whole
    FROM eligible
    WHERE dept_name IS NOT NULL
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY dept_name, level ORDER BY budget DESC NULLS LAST, id) AS dedup_rank
    FROM cleaned
    QUALIFY dedup_rank = 1
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY dept_name ORDER BY dedup_rank, id) AS rank_in_group
    FROM latest
    QUALIFY rank_in_group = 1
)
-- Multi-level aggregation
SELECT
    level,
    PERCENTILE_APPROX(budget, ARRAY(0.25, 0.5, 0.75)) AS budget_quartiles,
    COUNT(*) FILTER (WHERE level = 'L1') AS l1_rows,
    COUNT(*) AS row_count
FROM ranked
GROUP BY level
ORDER BY row_count DESC;
