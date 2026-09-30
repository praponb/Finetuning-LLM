select
    e.id,
    e.dept_id as employee_dept_id,
    m.salary as manager_salary,
    m.dept_id as manager_dept_id,
    m.level as manager_level
from employees e
left outer join employees m on e.manager_id = m.id
where e.salary >= 117000
order by 3 desc, id
limit 20;
