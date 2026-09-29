SELECT *, NTILE(5) OVER (ORDER BY budget ASC, id ASC) AS budget_bucket, PERCENT_RANK() OVER (PARTITION BY location, dept_name ORDER BY budget ASC NULLS LAST) AS budget_percent_rank FROM departments
