WITH eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM departments s
    WHERE NOT EXISTS (
            SELECT 1
            FROM employees c
            WHERE c.dept_id = s.id
              AND c.job_title = 'Intern'
        )
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        dept_name,
        location,
        budget,
        LAG(budget) OVER (PARTITION BY dept_name ORDER BY budget ASC, id ASC) AS prev_budget,
        LEAD(budget) OVER (PARTITION BY dept_name ORDER BY budget ASC, id ASC) AS next_budget,
        ROW_NUMBER() OVER (PARTITION BY dept_name ORDER BY budget ASC, id ASC) AS seq_no
    FROM eligible
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM metrics s
    WHERE NOT EXISTS (
            SELECT 1
            FROM employees ch
            WHERE ch.dept_id = s.id
              AND ch.hire_date BETWEEN '2025-04-01' AND '2025-06-06'
        )
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        PERCENT_RANK() OVER (ORDER BY budget DESC NULLS FIRST) AS budget_percent_rank,
        NTILE(4) OVER (ORDER BY budget ASC, id ASC) AS budget_bucket,
        LAST_VALUE(prev_budget, TRUE) OVER w_grp AS last_known_prev_budget,
        ROW_NUMBER() OVER (ORDER BY prev_budget ASC, id ASC) AS row_seq
    FROM kept
    WINDOW w_grp AS (PARTITION BY location ORDER BY seq_no ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CASE WHEN last_known_prev_budget >= 3085000 THEN 'High' WHEN last_known_prev_budget >= 1438000 THEN 'Medium' ELSE 'Low' END AS last_known_prev_budget_tier,
        COALESCE(dept_name, 'NONE') AS resolved_dept_name
    FROM with_windows
    WHERE seq_no >= 3
)
-- Native SQL PIVOT
SELECT *
FROM (
    SELECT
        dept_name,
        location,
        next_budget
    FROM filtered
) src
PIVOT (
    MAX(next_budget)
    FOR location IN ('Bangkok', 'Tokyo')
);
