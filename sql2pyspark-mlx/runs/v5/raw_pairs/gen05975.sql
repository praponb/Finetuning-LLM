select d.budget, t.min_salary, t.salary_range
from departments d
left join (select dept_id, min(salary) as min_salary, max(salary) - min(salary) as salary_range
    from employees
    where salary >= 144000
    group by dept_id) t on d.id = t.dept_id;
