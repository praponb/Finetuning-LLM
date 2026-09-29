SELECT
    id,
    dept_name,
    budget,
    location,
    FIRST_VALUE(location) OVER (PARTITION BY dept_name ORDER BY budget, id) AS first_location,
    DENSE_RANK() OVER (PARTITION BY dept_name ORDER BY budget) AS dense_rnk
FROM departments
