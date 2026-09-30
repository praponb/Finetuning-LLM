WITH base AS (
    -- Keep recent, valid rows
    SELECT
        *,
        LOWER(location) AS location_lower,
        CEIL(budget) AS budget_whole,
        COALESCE(location, 'NONE') AS location_clean
    FROM departments
    WHERE location IS NOT DISTINCT FROM 'Bangkok'
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        ROW_NUMBER() OVER w_recent AS seq_no,
        SUM(budget) OVER (PARTITION BY location, dept_name ORDER BY budget_whole, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_budget,
        ROUND(budget / SUM(budget) OVER (PARTITION BY location), 4) AS budget_share,
        LEAD(budget, 2) OVER w_hist AS next_budget,
        LEAD(budget, 2) OVER (PARTITION BY location ORDER BY budget, id) AS next_budget_2
    FROM base
    WINDOW w_recent AS (ORDER BY budget_whole, id), w_hist AS (PARTITION BY dept_name ORDER BY budget_whole, id)
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM enriched s
    WHERE NOT EXISTS (
            SELECT 1
            FROM employees x
            WHERE x.dept_id = s.id
              AND x.manager_id = 39
        )
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        dept_name,
        budget,
        location_lower,
        budget_whole,
        seq_no
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY dept_name ORDER BY running_budget DESC NULLS LAST, id) AS dedup_rank
        FROM not_flagged
    ) t
    WHERE dedup_rank = 1
)
SELECT
    id,
    dept_name,
    budget,
    seq_no
FROM latest
ORDER BY seq_no DESC NULLS LAST;
