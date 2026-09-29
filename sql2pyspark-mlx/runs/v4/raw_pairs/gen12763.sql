select location, dept_name, budget, trim(dept_name) as clean_dept_name from departments where (budget < 2406000 and dept_name in ('Finance', 'Legal', 'Sales')) or budget between 2874000 and 3759000
