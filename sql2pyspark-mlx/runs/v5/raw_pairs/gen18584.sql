SELECT s.* FROM departments s WHERE NOT EXISTS (SELECT 1 FROM employees ch WHERE ch.dept_id = s.id AND ch.salary <= 113000);
