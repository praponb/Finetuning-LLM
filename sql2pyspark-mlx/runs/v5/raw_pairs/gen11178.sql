WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CAST(budget AS INT) AS budget_int,
        COALESCE(budget, 0) AS budget_filled,
        COALESCE(dept_name, 'DIRECT') AS dept_name_clean
    FROM departments
    WHERE location NOT IN ('Singapore', 'New York')
      AND dept_name IS NOT NULL
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        dept_name,
        location,
        budget_int,
        dept_name_clean,
        ROW_NUMBER() OVER (ORDER BY budget_int DESC, id) AS row_seq,
        SUM(budget_int) OVER (PARTITION BY dept_name ORDER BY budget_filled, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_budget_int,
        NTILE(3) OVER (PARTITION BY dept_name ORDER BY budget_filled DESC, id) AS budget_filled_bucket
    FROM filtered
    WHERE location IN ('London', 'Bangkok', 'Chiang Mai')
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY location ORDER BY budget_int DESC, id) AS rn
        FROM with_windows
    ) t
    WHERE rn = 1
),
with_windows_departments AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        dept_name,
        budget_int,
        row_seq,
        budget_filled_bucket,
        DENSE_RANK() OVER (PARTITION BY dept_name, location ORDER BY cumulative_budget_int NULLS LAST) AS cumulative_budget_int_dense_rank,
        MIN(cumulative_budget_int) OVER (PARTITION BY dept_name) AS dept_name_min_cumulative_budget_int,
        NTILE(3) OVER (PARTITION BY dept_name ORDER BY cumulative_budget_int DESC, id) AS cumulative_budget_int_bucket
    FROM latest
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM with_windows_departments
    QUALIFY RANK() OVER (PARTITION BY dept_name ORDER BY dept_name_min_cumulative_budget_int DESC) = 1
),
remaining_keys AS (
    -- Set difference on keys
    SELECT id
    FROM ranked
    EXCEPT DISTINCT
    SELECT id
    FROM departments
    WHERE budget <= 2273000
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM ranked s
    JOIN remaining_keys k ON s.id = k.id
)
-- Window metrics over several different partitions
SELECT
    id,
    budget_int,
    row_seq,
    dept_name_min_cumulative_budget_int,
    RANK() OVER (PARTITION BY dept_name ORDER BY row_seq NULLS LAST) AS row_seq_rank,
    ROUND(cumulative_budget_int_dense_rank / SUM(cumulative_budget_int_dense_rank) OVER (PARTITION BY dept_name), 4) AS cumulative_budget_int_dense_rank_share,
    ROUND(cumulative_budget_int_dense_rank / SUM(cumulative_budget_int_dense_rank) OVER (PARTITION BY dept_name), 4) AS cumulative_budget_int_dense_rank_share_2,
    CUME_DIST() OVER (PARTITION BY dept_name ORDER BY budget_int NULLS FIRST) AS budget_int_cume_dist,
    MAX(dept_name_min_cumulative_budget_int) OVER (PARTITION BY dept_name) AS dept_name_max_dept_name_min_cumulative_budget_int
FROM verified
