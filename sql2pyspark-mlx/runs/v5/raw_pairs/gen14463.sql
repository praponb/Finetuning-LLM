SELECT
    id,
    dept_name,
    STDDEV_SAMP(budget) OVER (PARTITION BY location) location_stddev_samp_budget,
    ROUND(budget / SUM(budget) OVER (PARTITION BY dept_name, location), 4) budget_share,
    LEAD(budget, 1) OVER (PARTITION BY location, dept_name ORDER BY budget, id) next_budget
FROM departments;
