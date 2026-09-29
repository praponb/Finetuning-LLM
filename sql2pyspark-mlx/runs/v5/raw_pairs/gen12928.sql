SELECT src.*, d.budget, d.location, d.dept_name FROM employees src JOIN departments d ON src.dept_id = d.id WHERE d.dept_name = 'Legal';
