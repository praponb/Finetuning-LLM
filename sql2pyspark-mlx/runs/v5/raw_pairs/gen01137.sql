SELECT * FROM (SELECT dept_id, level, salary FROM employees) src PIVOT (AVG(salary) FOR level IN ('L4' AS l4, 'L1' AS l1));
