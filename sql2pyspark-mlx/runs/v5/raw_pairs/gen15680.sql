WITH base AS (
    SELECT
        *,
        SUBSTRING(dept_name, 1, 2) AS dept_name_prefix,
        COALESCE(dept_name, 'NONE') AS dept_name_filled
    FROM departments
    WHERE dept_name IS NOT NULL
      AND location IS DISTINCT FROM 'Chiang Mai'
),
ranked AS (
    SELECT *, RANK() OVER (PARTITION BY location ORDER BY budget DESC) AS rank_in_group
    FROM base
    QUALIFY rank_in_group = 1
),
metrics AS (
    SELECT
        id,
        budget,
        rank_in_group,
        PERCENT_RANK() OVER (PARTITION BY location ORDER BY budget DESC) AS budget_percent_rank,
        RANK() OVER (PARTITION BY location ORDER BY budget DESC) AS budget_rank,
        NTILE(4) OVER (ORDER BY budget DESC, id) AS budget_bucket,
        FIRST_VALUE(location, TRUE) OVER (PARTITION BY location ORDER BY rank_in_group, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_location,
        SUM(budget) OVER (ORDER BY budget, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_budget
    FROM ranked
    WHERE rank_in_group < 1
),
latest_match AS (
    SELECT
        s.*,
        latest.employee_hire_date,
        latest.salary
    FROM metrics s
    LEFT JOIN LATERAL (
            SELECT c.hire_date AS employee_hire_date, c.salary
            FROM employees c
            WHERE c.dept_id = s.id
            ORDER BY c.salary DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
kept_keys AS (
    SELECT id
    FROM latest_match
    EXCEPT ALL
    SELECT id
    FROM departments
    WHERE dept_name IN ('Engineering', 'Finance')
),
kept_rows AS (
    SELECT s.*
    FROM latest_match s
    INNER JOIN kept_keys k ON s.id = k.id
),
kept AS (
    SELECT s.*
    FROM kept_rows s
    WHERE NOT EXISTS (
            SELECT 1
            FROM employees c
            WHERE c.dept_id = s.id
              AND c.hire_date <= s.employee_hire_date
        )
)
SELECT
    budget_rank,
    budget_bucket,
    first_known_location,
    employee_hire_date
FROM kept
ORDER BY employee_hire_date DESC;
