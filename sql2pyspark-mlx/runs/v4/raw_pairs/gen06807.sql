SELECT
    id,
    dept_name,
    location,
    budget,
    AVG(budget) OVER (ORDER BY budget, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS budget_avg_2_prior,
    PERCENT_RANK() OVER (PARTITION BY location ORDER BY budget DESC NULLS FIRST) AS budget_percent_rank,
    ROUND(budget / SUM(budget) OVER (PARTITION BY location), 4) AS budget_share,
    NTILE(3) OVER (PARTITION BY location ORDER BY budget, id) AS budget_bucket,
    ROW_NUMBER() OVER (PARTITION BY location ORDER BY budget, id) AS event_seq
FROM departments
