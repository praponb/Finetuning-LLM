select d.location, t.total_salary
from departments d
inner join (select dept_id, sum(salary) total_salary
    from employees
    group by dept_id) t on d.id = t.dept_id
order by location, t.total_salary;
