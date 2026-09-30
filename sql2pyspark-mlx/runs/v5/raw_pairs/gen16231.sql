select id, manager_id, count(*) over (partition by manager_id) manager_id_count from employees where hire_date < '2023-08-01' order by manager_id desc, manager_id_count
