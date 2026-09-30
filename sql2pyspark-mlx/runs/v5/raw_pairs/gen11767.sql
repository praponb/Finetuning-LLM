WITH filtered AS (
    -- Keep recent, valid rows
    SELECT *, CASE WHEN budget >= 3498000 THEN 'High' WHEN budget >= 1900000 THEN 'Medium' ELSE 'Low' END AS budget_tier
    FROM departments
    WHERE dept_name IS NOT NULL
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        dept_name,
        location,
        MAX(budget) OVER (PARTITION BY location, dept_name ORDER BY budget, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_max_budget,
        MIN(budget) OVER (PARTITION BY location, dept_name ORDER BY budget, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_min_budget,
        COUNT(budget) OVER (PARTITION BY location, dept_name ORDER BY budget, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_count_budget,
        AVG(budget) OVER (PARTITION BY location, dept_name ORDER BY budget, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_avg_budget,
        LAST_VALUE(budget) IGNORE NULLS OVER (PARTITION BY dept_name ORDER BY budget, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_budget
    FROM filtered
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY location ORDER BY last4_count_budget DESC NULLS LAST, id) AS dedup_rank
        FROM enriched
    ) x
    WHERE dedup_rank = 1
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        last4_min_budget,
        last4_avg_budget,
        AVG(last4_avg_budget) OVER (PARTITION BY location, dept_name ORDER BY last4_max_budget, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_last4_avg_budget,
        SUM(last4_avg_budget) OVER (PARTITION BY location, dept_name ORDER BY last4_max_budget, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_last4_avg_budget,
        MAX(last4_avg_budget) OVER (PARTITION BY location, dept_name ORDER BY last4_max_budget, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_last4_avg_budget,
        RANK() OVER (PARTITION BY location ORDER BY last4_min_budget NULLS LAST) AS last4_min_budget_rank
    FROM deduped
),
metrics_departments AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        running_avg_last4_avg_budget,
        running_max_last4_avg_budget,
        SUM(last4_min_budget_rank) OVER (ORDER BY last4_min_budget, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_last4_min_budget_rank,
        AVG(last4_min_budget_rank) OVER (ORDER BY running_max_last4_avg_budget, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS last4_min_budget_rank_avg_2_prior,
        FIRST_VALUE(last4_min_budget_rank) IGNORE NULLS OVER (ORDER BY last4_min_budget_rank, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_last4_min_budget_rank
    FROM metrics
    WHERE last4_avg_budget <= 3046000
)
-- Window metrics over several different partitions
SELECT
    id,
    first_known_last4_min_budget_rank,
    LEAD(last4_min_budget_rank_avg_2_prior) OVER (ORDER BY cumulative_last4_min_budget_rank, id) AS next_last4_min_budget_rank_avg_2_prior,
    LEAD(cumulative_last4_min_budget_rank) OVER (ORDER BY cumulative_last4_min_budget_rank, id) AS next_cumulative_last4_min_budget_rank,
    LAG(cumulative_last4_min_budget_rank) OVER (ORDER BY cumulative_last4_min_budget_rank, id) AS prev_cumulative_last4_min_budget_rank,
    ROW_NUMBER() OVER (ORDER BY cumulative_last4_min_budget_rank, id) AS event_seq
FROM metrics_departments;
