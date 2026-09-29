WITH enriched AS (
    SELECT
        id,
        dept_name,
        location,
        budget,
        PERCENT_RANK() OVER (PARTITION BY location ORDER BY budget DESC NULLS FIRST) AS budget_percent_rank,
        COUNT(budget) OVER (PARTITION BY location) AS location_count_budget,
        SUM(budget) OVER w_running AS cumulative_budget,
        ROW_NUMBER() OVER (PARTITION BY dept_name ORDER BY budget, id) AS seq_no,
        LAST_VALUE(budget, TRUE) OVER (ORDER BY budget, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_budget
    FROM departments
    WINDOW w_running AS (PARTITION BY location, dept_name ORDER BY budget, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
)
SELECT
    id,
    dept_name,
    location_count_budget,
    last_known_budget
FROM enriched
ORDER BY dept_name DESC
