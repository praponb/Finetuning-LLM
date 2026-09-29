WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        ROUND(budget, 2) AS budget_rounded,
        COALESCE(dept_name, 'NONE') AS dept_name_filled
    FROM departments
    WHERE location IS NOT NULL
      AND location IS DISTINCT FROM 'Berlin'
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM filtered s
    WHERE NOT EXISTS (
            SELECT 1
            FROM employees x
            WHERE x.dept_id = s.id
              AND x.email LIKE '%@outlook.com'
        )
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        location,
        budget,
        budget_rounded,
        dept_name_filled,
        SUM(budget) OVER (PARTITION BY location, dept_name ORDER BY budget_rounded, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_sum_budget,
        AVG(budget) OVER (PARTITION BY location, dept_name ORDER BY budget_rounded, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_avg_budget,
        COUNT(budget) OVER (PARTITION BY location, dept_name ORDER BY budget_rounded, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_count_budget,
        SUM(budget_rounded) OVER (ORDER BY budget, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_budget_rounded
    FROM not_flagged
    WHERE dept_name = 'Sales'
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM with_windows s
    WHERE NOT EXISTS (
            SELECT 1
            FROM employees ch
            WHERE ch.dept_id = s.id
              AND ch.salary < 92000
        )
),
with_windows_departments AS (
    -- Ranking and gap features
    SELECT
        *,
        LEAD(last4_count_budget, 2) OVER w_hist AS next_last4_count_budget,
        AVG(running_budget_rounded) OVER (ORDER BY running_budget_rounded, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS running_budget_rounded_avg_2,
        LAG(budget, 1) OVER w_hist AS prev_budget,
        AVG(budget_rounded) OVER (PARTITION BY location ORDER BY last4_count_budget, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS budget_rounded_avg_3_prior,
        SUM(last4_avg_budget) OVER w_hist_2 AS running_last4_avg_budget
    FROM kept
    WINDOW w_hist AS (PARTITION BY location ORDER BY budget, id), w_hist_2 AS (PARTITION BY location ORDER BY running_budget_rounded, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LAST_VALUE(budget_rounded_avg_3_prior, TRUE) OVER (PARTITION BY location ORDER BY last4_avg_budget, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_budget_rounded_avg_3_prior,
        ROW_NUMBER() OVER (PARTITION BY location ORDER BY prev_budget DESC, id) AS row_seq,
        LAG(last4_count_budget, 1) OVER (PARTITION BY location ORDER BY budget, id) AS prev_last4_count_budget
    FROM with_windows_departments
),
filtered_departments AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        COALESCE(last4_avg_budget, 0) AS last4_avg_budget_filled,
        ROUND(last4_avg_budget, 1) AS last4_avg_budget_rounded
    FROM scored
    WHERE running_budget_rounded_avg_2 IS NULL
)
-- Native SQL UNPIVOT (wide to long)
SELECT *
FROM (
    SELECT
        id,
        location,
        budget_rounded,
        last4_avg_budget_rounded,
        budget
    FROM filtered_departments
) src
UNPIVOT (
    val FOR kpi IN (budget_rounded, last4_avg_budget_rounded, budget)
) unp
