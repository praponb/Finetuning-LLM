SELECT * FROM (SELECT manager_id, level, salary FROM employees) src PIVOT (SUM(salary) AS total, COUNT(1) AS cnt FOR level IN ('L4', 'L1', 'L5'));
