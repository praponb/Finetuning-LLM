SELECT id, budget, CONCAT_WS('-', location, dept_name) AS location_dept_name_key FROM departments WHERE budget BETWEEN 3337000 AND 3590000 ORDER BY budget, id LIMIT 20;
