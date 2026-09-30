select id, hire_date, level from employees where full_name like '%Lee%' union all select id, hire_date, level from employees where salary <= 74000 order by id desc
