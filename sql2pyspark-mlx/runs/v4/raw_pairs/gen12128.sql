WITH ranked AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY level ORDER BY salary, id) AS pos FROM employees) SELECT id, level, salary, manager_id FROM ranked WHERE pos <= 5 ORDER BY id DESC;
