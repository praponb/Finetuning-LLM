WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        SUBSTRING(location, 1, 2) location_prefix,
        COALESCE(location, 'N/A') location_filled
    FROM departments
    WHERE location IS NOT NULL
      AND location = 'London'
),
valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        dept_name,
        budget,
        location_prefix,
        location_filled,
        CASE WHEN dept_name IN ('Engineering', 'Legal') THEN 'Tier 1' ELSE 'Other' END dept_name_group
    FROM base
    WHERE location != 'Singapore'
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        LEAD(budget) OVER (PARTITION BY dept_name ORDER BY budget ASC, id ASC) next_budget,
        LAG(budget) OVER (PARTITION BY dept_name ORDER BY budget ASC, id ASC) prev_budget,
        ROW_NUMBER() OVER (PARTITION BY dept_name ORDER BY budget ASC, id ASC) event_seq,
        CUME_DIST() OVER (PARTITION BY dept_name ORDER BY budget DESC NULLS FIRST) budget_cume_dist
    FROM valid
    WHERE dept_name = 'Marketing'
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        dept_name,
        budget,
        location_filled,
        dept_name_group,
        prev_budget,
        event_seq
    FROM with_windows
    QUALIFY ROW_NUMBER() OVER (PARTITION BY dept_name ORDER BY next_budget DESC NULLS FIRST, id ASC) = 1
),
eligible AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM deduped s
    WHERE EXISTS (
            SELECT 1
            FROM employees c
            WHERE c.dept_id = s.id
              AND c.job_title = 'Manager'
        )
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM eligible s
    WHERE NOT EXISTS (
            SELECT 1
            FROM employees c
            WHERE c.dept_id = s.id
              AND c.dept_id IN (3, 8, 9)
        )
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        dept_name,
        budget,
        location_filled,
        dept_name_group,
        prev_budget,
        LAST_VALUE(prev_budget) IGNORE NULLS OVER (PARTITION BY dept_name ORDER BY event_seq ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) last_known_prev_budget,
        NTILE(10) OVER (PARTITION BY dept_name ORDER BY event_seq ASC, id ASC) event_seq_bucket,
        ROUND(event_seq / SUM(event_seq) OVER (PARTITION BY dept_name), 4) event_seq_share
    FROM kept
)
SELECT
    id,
    budget,
    location_filled,
    prev_budget,
    last_known_prev_budget,
    event_seq_bucket,
    event_seq_share
FROM enriched
ORDER BY event_seq_bucket DESC
