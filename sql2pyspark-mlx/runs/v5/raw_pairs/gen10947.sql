select e.hire_date, e.dept_id, d.dept_name, d.budget, round(e.salary, 0) as salary_rounded from employees e full outer join departments d on e.dept_id = d.id;
