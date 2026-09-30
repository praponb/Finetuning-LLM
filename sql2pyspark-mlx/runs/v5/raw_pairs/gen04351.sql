WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, FLOOR(budget) AS budget_whole
    FROM departments
    WHERE location IS NOT NULL
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        AVG(budget_whole) OVER (PARTITION BY dept_name ORDER BY budget_whole, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS budget_whole_avg_5,
        MAX(budget_whole) OVER w_running AS dept_name_max_budget_whole,
        LAG(budget, 1) OVER w_hist AS prev_budget,
        AVG(budget_whole) OVER w_grp AS budget_whole_avg_5_2
    FROM valid
    WINDOW w_running AS (PARTITION BY dept_name), w_hist AS (PARTITION BY dept_name, location ORDER BY budget_whole, id), w_grp AS (PARTITION BY location, dept_name ORDER BY budget_whole, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW)
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY location, dept_name ORDER BY dept_name_max_budget_whole DESC, id) AS latest_rank
        FROM enriched
    ) ranked
    WHERE latest_rank = 1
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        dept_name,
        budget,
        budget_whole,
        prev_budget,
        AVG(budget_whole) OVER w_part AS budget_whole_avg_2_prior,
        DENSE_RANK() OVER (PARTITION BY location ORDER BY prev_budget DESC NULLS LAST) AS prev_budget_dense_rank,
        LEAD(budget, 1) OVER w_recent AS next_budget,
        LEAD(budget_whole_avg_5, 1) OVER w_hist AS next_budget_whole_avg_5
    FROM deduped
    WINDOW w_part AS (PARTITION BY dept_name, location ORDER BY budget_whole, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), w_recent AS (PARTITION BY location ORDER BY dept_name_max_budget_whole, id), w_hist AS (PARTITION BY dept_name, location ORDER BY latest_rank, id)
)
SELECT
    id,
    dept_name,
    budget,
    prev_budget,
    budget_whole_avg_2_prior,
    next_budget,
    next_budget_whole_avg_5
FROM metrics
