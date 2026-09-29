WITH filtered AS (
    SELECT *, LENGTH(dept_name) dept_name_length
    FROM departments
    WHERE dept_name IS NOT NULL
)
SELECT
    id,
    dept_name,
    location,
    budget,
    dept_name_length,
    LEAD(budget, 2) OVER (PARTITION BY dept_name ORDER BY dept_name_length, id) next_budget,
    AVG(budget) OVER w_part budget_avg_3_prior,
    LAG(budget, 2) OVER (ORDER BY budget, id) prev_budget
FROM filtered
WINDOW w_part AS (ORDER BY budget, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING)
