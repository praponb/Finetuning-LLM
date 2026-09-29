select d.dept_name, t.avg_salary
from departments d
left outer join (select dept_id, avg(salary) as avg_salary
    from employees
    group by dept_id) t on d.id = t.dept_id
