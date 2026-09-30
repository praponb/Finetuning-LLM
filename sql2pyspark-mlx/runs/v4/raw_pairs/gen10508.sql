SELECT *, RANK() OVER (PARTITION BY manager_id, level ORDER BY hire_date DESC) AS dedup_rank
FROM employees
QUALIFY dedup_rank = 1;
