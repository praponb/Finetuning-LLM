WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        *,
        COALESCE(location, 'Unknown') AS location_filled,
        COALESCE(budget, 0) AS budget_filled,
        COALESCE(location, 'DIRECT') AS location_clean
    FROM departments
    WHERE location IS NOT NULL
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LAST_VALUE(budget_filled) IGNORE NULLS OVER (PARTITION BY dept_name ORDER BY budget, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_budget_filled,
        FIRST_VALUE(budget) IGNORE NULLS OVER (ORDER BY budget, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_budget,
        NTILE(4) OVER w_grp AS budget_bucket
    FROM cleaned
    WINDOW w_grp AS (PARTITION BY location ORDER BY budget DESC, id)
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY dept_name ORDER BY first_known_budget DESC NULLS FIRST, id) AS latest_rank
    FROM with_windows
    QUALIFY latest_rank = 1
),
with_windows_departments AS (
    -- Ranking and gap features
    SELECT
        id,
        dept_name,
        location,
        budget,
        location_filled,
        budget_filled,
        location_clean,
        last_known_budget_filled,
        first_known_budget,
        budget_bucket,
        latest_rank,
        LAG(first_known_budget, 1) OVER (PARTITION BY location, dept_name ORDER BY first_known_budget, id) AS prev_first_known_budget,
        ROW_NUMBER() OVER (PARTITION BY dept_name ORDER BY budget_bucket DESC, id) AS seq_no,
        AVG(budget) OVER (PARTITION BY dept_name ORDER BY last_known_budget_filled, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS budget_avg_6_prior
    FROM deduped
)
SELECT
    id,
    location,
    budget,
    location_filled,
    budget_filled,
    location_clean,
    last_known_budget_filled,
    budget_bucket,
    prev_first_known_budget
FROM with_windows_departments
ORDER BY budget_bucket, last_known_budget_filled DESC;
