SELECT * FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY job_title ORDER BY salary DESC, id) AS rnk FROM employees) AS r WHERE rnk <= 3;
