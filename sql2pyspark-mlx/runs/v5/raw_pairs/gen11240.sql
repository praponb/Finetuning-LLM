WITH valid AS (
    SELECT
        *,
        UPPER(dept_name) AS dept_name_upper,
        ROUND(budget, 2) AS budget_rounded
    FROM departments
    WHERE location IS NOT NULL
      AND location = 'Berlin'
),
with_windows AS (
    SELECT
        id,
        dept_name,
        dept_name_upper,
        budget_rounded,
        AVG(budget_rounded) OVER w_part AS running_avg_budget_rounded,
        COUNT(budget_rounded) OVER w_part AS running_count_budget_rounded,
        SUM(budget_rounded) OVER w_part AS running_sum_budget_rounded,
        MAX(budget_rounded) OVER w_part AS running_max_budget_rounded,
        PERCENT_RANK() OVER (PARTITION BY location, dept_name ORDER BY budget ASC) AS budget_percent_rank
    FROM valid
    WHERE location != 'Singapore'
    WINDOW w_part AS (PARTITION BY location, dept_name ORDER BY budget_rounded ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
kept_keys AS (
    SELECT dept_name, id
    FROM with_windows
    INTERSECT ALL
    SELECT dept_name, id
    FROM departments
    WHERE location != 'New York'
),
verified AS (
    SELECT s.*
    FROM with_windows s
    INNER JOIN kept_keys k ON s.dept_name = k.dept_name AND s.id = k.id
),
eligible AS (
    SELECT s.*
    FROM verified s
    WHERE NOT EXISTS (
            SELECT 1
            FROM employees x
            WHERE x.dept_id = s.id
              AND x.level IN ('L4', 'L5')
        )
),
valid_departments AS (
    SELECT
        dept_name,
        dept_name_upper,
        budget_rounded,
        running_avg_budget_rounded,
        running_sum_budget_rounded,
        running_max_budget_rounded,
        budget_percent_rank,
        COALESCE(budget_percent_rank, 0) AS budget_percent_rank_filled,
        COALESCE(dept_name, 'NONE') AS dept_name_filled
    FROM eligible
    WHERE dept_name IS NOT DISTINCT FROM 'Marketing'
      AND dept_name IS NOT NULL
)
SELECT
    dept_name,
    dept_name_upper,
    running_avg_budget_rounded,
    running_max_budget_rounded,
    budget_percent_rank,
    budget_percent_rank_filled,
    dept_name_filled
FROM valid_departments
ORDER BY running_max_budget_rounded DESC;
