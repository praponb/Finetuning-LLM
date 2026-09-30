WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, CAST(budget AS INT) AS budget_int
    FROM departments
    WHERE dept_name IS NOT NULL
      AND budget <= 1161000
),
matched_keys AS (
    -- Set difference on keys
    SELECT location, dept_name
    FROM filtered
    EXCEPT
    SELECT location, dept_name
    FROM departments
    WHERE dept_name NOT IN ('Finance', 'Engineering')
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM filtered s
    INNER JOIN matched_keys k ON s.location = k.location AND s.dept_name = k.dept_name
),
eligible AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM verified s
    WHERE EXISTS (
            SELECT 1
            FROM employees x
            WHERE x.dept_id = s.id
              AND x.full_name LIKE '%Carol%'
        )
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        PERCENT_RANK() OVER w_running AS budget_percent_rank,
        MAX(budget_int) OVER (PARTITION BY dept_name) AS dept_name_max_budget_int
    FROM eligible
    WINDOW w_running AS (PARTITION BY location ORDER BY budget NULLS LAST)
),
with_windows_departments AS (
    -- Ranking and gap features
    SELECT
        *,
        DENSE_RANK() OVER (ORDER BY budget_percent_rank DESC NULLS FIRST) AS budget_percent_rank_dense_rank,
        AVG(budget_percent_rank) OVER (PARTITION BY location) AS location_avg_budget_percent_rank
    FROM with_windows
),
matched_keys_2 AS (
    -- Set difference on keys
    SELECT id
    FROM with_windows_departments
    EXCEPT DISTINCT
    SELECT id
    FROM departments
    WHERE budget < 2125000
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM with_windows_departments s
    INNER JOIN matched_keys_2 k ON s.id = k.id
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        dept_name,
        location,
        budget,
        budget_int,
        budget_percent_rank,
        dept_name_max_budget_int,
        budget_percent_rank_dense_rank,
        location_avg_budget_percent_rank,
        RANK() OVER w_hist AS budget_int_rank,
        COUNT(location_avg_budget_percent_rank) OVER w_hist_2 AS dept_name_count_location_avg_budget_percent_rank,
        DENSE_RANK() OVER (PARTITION BY location, dept_name ORDER BY budget_percent_rank_dense_rank NULLS LAST) AS budget_percent_rank_dense_rank_dense_rank,
        RANK() OVER w_ordered AS dept_name_max_budget_int_rank,
        RANK() OVER w_ordered_2 AS budget_rank
    FROM kept_rows
    WINDOW w_hist AS (PARTITION BY location, dept_name ORDER BY budget_int), w_hist_2 AS (PARTITION BY dept_name), w_ordered AS (PARTITION BY location ORDER BY dept_name_max_budget_int DESC NULLS FIRST), w_ordered_2 AS (PARTITION BY dept_name, location ORDER BY budget DESC NULLS FIRST)
),
stats AS (
    -- Grouped metrics
    SELECT
        CASE WHEN budget_percent_rank_dense_rank >= 6 THEN 'HIGH' ELSE 'LOW' END AS budget_percent_rank_dense_rank_tier,
        PERCENTILE_APPROX(dept_name_max_budget_int, 0.95) AS dept_name_max_budget_int_p95,
        SIZE(COLLECT_SET(dept_name)) AS num_dept_names
    FROM scored
    GROUP BY CASE WHEN budget_percent_rank_dense_rank >= 6 THEN 'HIGH' ELSE 'LOW' END
    HAVING num_dept_names >= 1
)
SELECT budget_percent_rank_dense_rank_tier
FROM stats
