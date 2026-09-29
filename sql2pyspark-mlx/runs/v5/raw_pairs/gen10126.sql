SELECT id, dept_name, budget FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY location, dept_name ORDER BY budget DESC, id ASC) AS latest_rank FROM departments) t WHERE latest_rank = 1
