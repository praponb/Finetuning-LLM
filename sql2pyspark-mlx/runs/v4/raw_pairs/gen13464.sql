WITH scored AS (
    SELECT
        *,
        AVG(budget) OVER (PARTITION BY dept_name ORDER BY budget, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS budget_avg_3_prior,
        PERCENT_RANK() OVER (PARTITION BY dept_name ORDER BY budget NULLS FIRST) AS budget_percent_rank
    FROM departments
)
SELECT
    id,
    dept_name,
    budget,
    budget_avg_3_prior,
    budget_percent_rank
FROM scored;
