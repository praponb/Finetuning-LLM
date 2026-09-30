WITH scored AS (
    -- Ranking and gap features
    SELECT
        id,
        dept_name,
        location,
        budget,
        LEAD(budget) OVER w_hist next_budget,
        LAG(budget) OVER w_hist prev_budget,
        ROW_NUMBER() OVER w_hist row_seq
    FROM departments
    WHERE dept_name = 'Sales'
    WINDOW w_hist AS (PARTITION BY location ORDER BY budget, id)
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        MIN(budget) OVER w_grp last7_min_budget,
        MAX(budget) OVER w_grp last7_max_budget,
        AVG(budget) OVER w_grp last7_avg_budget
    FROM scored
    WINDOW w_grp AS (PARTITION BY dept_name ORDER BY next_budget, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)
),
matched_keys AS (
    -- Set difference on keys
    SELECT location, dept_name
    FROM metrics
    EXCEPT
    SELECT location, dept_name
    FROM departments
    WHERE budget BETWEEN 3427000 AND 3724000
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM metrics s
    INNER JOIN matched_keys k ON s.location = k.location AND s.dept_name = k.dept_name
),
kept AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM reconciled s
    WHERE EXISTS (
            SELECT 1
            FROM employees ch
            WHERE ch.dept_id = s.id
              AND ch.manager_id = 33
        )
),
daily AS (
    -- Aggregate with conditional metrics
    SELECT
        CASE WHEN prev_budget >= 3781000 THEN 'LARGE' ELSE 'SMALL' END AS prev_budget_tier,
        CASE WHEN next_budget >= 4181000 THEN 'PREMIUM' ELSE 'STANDARD' END AS next_budget_tier,
        ROUND(AVG(next_budget), 2) AS avg_next_budget,
        COUNT(*) FILTER (WHERE location = 'London') AS london_rows,
        AVG(last7_min_budget) FILTER (WHERE location = 'London') AS london_avg_last7_min_budget,
        AVG(budget) AS avg_budget
    FROM kept
    GROUP BY CASE WHEN prev_budget >= 3781000 THEN 'LARGE' ELSE 'SMALL' END, CASE WHEN next_budget >= 4181000 THEN 'PREMIUM' ELSE 'STANDARD' END
)
SELECT
    prev_budget_tier,
    next_budget_tier,
    avg_next_budget,
    london_avg_last7_min_budget,
    avg_budget
FROM daily
ORDER BY london_avg_last7_min_budget NULLS LAST
