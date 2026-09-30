WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT *, ROUND(budget, 2) AS budget_rounded
    FROM departments
    WHERE location = 'Singapore'
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT
        id,
        dept_name,
        budget,
        budget_rounded
    FROM (
        SELECT *, RANK() OVER (PARTITION BY location ORDER BY budget DESC NULLS FIRST) AS rn
        FROM cleaned
    ) ranked
    WHERE rn = 1
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        STDDEV_SAMP(budget) OVER (PARTITION BY dept_name) AS dept_name_stddev_samp_budget,
        NTILE(10) OVER (PARTITION BY dept_name ORDER BY budget, id) AS budget_bucket,
        AVG(budget_rounded) OVER (PARTITION BY dept_name ORDER BY budget_rounded, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS budget_rounded_avg_2_prior,
        LAST_VALUE(dept_name) IGNORE NULLS OVER (PARTITION BY dept_name ORDER BY budget, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_dept_name,
        LEAD(budget_rounded, 2) OVER (ORDER BY budget, id) AS next_budget_rounded
    FROM deduped
),
metrics_departments AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        ROW_NUMBER() OVER (ORDER BY budget_bucket DESC, id) AS event_seq,
        LAST_VALUE(dept_name) IGNORE NULLS OVER (PARTITION BY dept_name ORDER BY dept_name_stddev_samp_budget, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_dept_name_2,
        LEAD(budget_bucket, 1) OVER (PARTITION BY dept_name ORDER BY next_budget_rounded, id) AS next_budget_bucket
    FROM metrics
),
valid AS (
    -- Keep recent, valid rows
    SELECT *, CASE WHEN budget_rounded_avg_2_prior >= 69 THEN 'High' WHEN budget_rounded_avg_2_prior >= 19 THEN 'Medium' ELSE 'Low' END AS budget_rounded_avg_2_prior_band
    FROM metrics_departments
    WHERE dept_name IS NOT NULL
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        CUME_DIST() OVER (ORDER BY event_seq DESC NULLS LAST) AS event_seq_cume_dist,
        RANK() OVER (ORDER BY budget_rounded_avg_2_prior NULLS FIRST) AS budget_rounded_avg_2_prior_rank
    FROM valid
)
SELECT
    id,
    budget,
    budget_rounded,
    budget_bucket,
    budget_rounded_avg_2_prior,
    last_known_dept_name,
    event_seq,
    next_budget_bucket,
    budget_rounded_avg_2_prior_band
FROM scored;
