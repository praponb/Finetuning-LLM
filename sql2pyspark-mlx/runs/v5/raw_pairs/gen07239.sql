SELECT id, location, dept_name, budget, DENSE_RANK() OVER (PARTITION BY location, dept_name ORDER BY budget DESC) AS dense_rnk FROM departments
