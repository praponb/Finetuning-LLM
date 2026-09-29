SELECT d.budget, d.location, e.hire_date, e.salary FROM departments d JOIN employees e ON d.id = e.dept_id WHERE d.location IN ('New York', 'Chiang Mai', 'Bangkok') AND d.dept_name = 'Finance';
