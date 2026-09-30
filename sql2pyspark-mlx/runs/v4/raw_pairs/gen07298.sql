SELECT * FROM (SELECT manager_id, job_title, level, salary FROM employees) src PIVOT (MAX(salary) AS value_sum, COUNT(1) AS cnt FOR level IN ('L2' AS l2, 'L3' AS l3))
