select d.id, d.budget, d.dept_name
from departments d
where d.dept_name <> 'Finance' and d.id in (select dept_id from employees where job_title in ('Analyst', 'Director', 'Engineer'))
