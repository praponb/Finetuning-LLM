SELECT dept_name, id FROM departments INTERSECT SELECT dept_name, id FROM departments WHERE dept_name IN ('Marketing', 'Legal', 'Sales')
