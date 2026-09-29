WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        *,
        ROUND(budget, 0) AS budget_rounded,
        COALESCE(location, 'OTHER') AS resolved_location
    FROM departments
    WHERE dept_name = 'Marketing'
),
not_flagged AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM cleaned s
    WHERE EXISTS (
            SELECT 1
            FROM employees c
            WHERE c.dept_id = s.id
              AND c.manager_id IS NOT NULL
        )
),
latest_match AS (
    -- Latest matching employee per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        top1.job_title,
        top1.level
    FROM not_flagged s
    LEFT JOIN LATERAL (
            SELECT c.job_title, c.level
            FROM employees c
            WHERE c.dept_id = s.id
            ORDER BY c.salary DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY job_title, level ORDER BY budget_rounded, id) AS dedup_rank
    FROM latest_match
    QUALIFY dedup_rank = 1
),
base AS (
    -- Keep recent, valid rows
    SELECT
        *,
        UPPER(dept_name) AS dept_name_upper,
        SUBSTRING(location, 1, 3) AS location_prefix,
        COALESCE(location, 'N/A') AS resolved_location_2
    FROM deduped
    WHERE location IN ('London', 'Tokyo')
)
-- Running and moving aggregates
SELECT
    *,
    LAG(budget) OVER (PARTITION BY level, dept_name ORDER BY dedup_rank, id) AS prev_budget,
    LEAD(budget_rounded) OVER (PARTITION BY level, dept_name ORDER BY dedup_rank, id) AS next_budget_rounded,
    ROW_NUMBER() OVER (PARTITION BY level, dept_name ORDER BY dedup_rank, id) AS row_seq,
    AVG(budget) OVER (PARTITION BY location ORDER BY budget, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS budget_avg_6
FROM base
