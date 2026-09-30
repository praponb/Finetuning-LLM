SELECT * FROM (SELECT *, RANK() OVER (PARTITION BY dept_name ORDER BY budget DESC NULLS FIRST) AS latest_rank FROM departments) x WHERE latest_rank = 1;
