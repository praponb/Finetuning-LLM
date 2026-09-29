select id, dept_id, count(*) over (partition by dept_id) dept_id_count from employees
