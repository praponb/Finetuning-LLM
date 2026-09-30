WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        *,
        CASE WHEN location = 'Bangkok' THEN 1 ELSE 0 END AS is_bangkok,
        COALESCE(location, 'NONE') AS resolved_location
    FROM departments
    WHERE location <> 'New York'
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY dept_name, location ORDER BY budget, id) AS dedup_rank
        FROM filtered
    ) t
    WHERE dedup_rank = 1
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROW_NUMBER() OVER (ORDER BY dedup_rank, id) AS event_seq,
        SUM(budget) OVER (PARTITION BY location ORDER BY budget, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_budget,
        NTILE(5) OVER (ORDER BY budget, id) AS budget_bucket
    FROM ranked
)
SELECT
    id,
    location,
    budget,
    is_bangkok,
    dedup_rank,
    event_seq,
    cumulative_budget,
    budget_bucket
FROM scored
