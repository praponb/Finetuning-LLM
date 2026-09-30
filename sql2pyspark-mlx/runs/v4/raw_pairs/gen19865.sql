select count(distinct salary) as unique_job_titles, min(salary) as min_salary
from employees
where not is_remote or full_name like 'bob%';
