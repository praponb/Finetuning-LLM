select e.id, e.dept_id, e.salary, e.hire_date from employees e where e.salary < (select avg(e2.salary) from employees e2 where e2.dept_id = e.dept_id);
