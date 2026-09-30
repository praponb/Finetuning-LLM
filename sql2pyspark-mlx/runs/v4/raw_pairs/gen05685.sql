SELECT *, ROW_NUMBER() OVER (PARTITION BY dept_name, location ORDER BY budget, id) AS latest_rank FROM departments QUALIFY latest_rank = 1;
