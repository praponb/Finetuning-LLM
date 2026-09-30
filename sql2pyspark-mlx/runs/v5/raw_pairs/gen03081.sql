select id, location, budget, row_number() over (partition by location order by budget desc, id) as rn from departments where dept_name not in ('Engineering', 'Finance', 'HR')
