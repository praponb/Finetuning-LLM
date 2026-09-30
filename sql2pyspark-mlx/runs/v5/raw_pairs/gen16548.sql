select id, manager_id, level, salary, upper(trim(full_name)) as full_name_normalized from employees where manager_id in (14, 15)
