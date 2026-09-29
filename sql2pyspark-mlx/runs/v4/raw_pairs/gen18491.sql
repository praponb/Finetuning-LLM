SELECT * FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY dept_id, level ORDER BY hire_date, id) AS rank_in_group FROM employees) ranked WHERE rank_in_group = 1;
