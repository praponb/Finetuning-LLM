select e.id, e.job_title, e.salary from employees e where e.level in ('L3', 'L2') and e.dept_id in (select id from departments where dept_name = 'Sales') order by job_title
