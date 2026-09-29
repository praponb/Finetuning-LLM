SELECT id, manager_id, salary, is_remote FROM (SELECT *, DENSE_RANK() OVER (PARTITION BY manager_id ORDER BY salary DESC) AS rnk FROM employees) AS t WHERE rnk = 1 ORDER BY id DESC;
