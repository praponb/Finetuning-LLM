select id, dept_name, length(dept_name) as dept_name_length, initcap(trim(dept_name)) as dept_name_title from departments where dept_name in ('Engineering', 'HR')
