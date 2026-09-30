select departments.id, departments.budget from departments where departments.id in (select dept_id from employees)
