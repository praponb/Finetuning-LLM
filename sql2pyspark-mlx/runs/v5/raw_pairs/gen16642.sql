select departments.id, departments.budget
from departments
where departments.budget > 3982000 and exists (select 1 from employees where employees.dept_id = departments.id)
order by departments.budget desc;
