SELECT id, dept_name FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY dept_name ORDER BY budget, id) AS dedup_rank FROM departments) ranked WHERE dedup_rank = 1
