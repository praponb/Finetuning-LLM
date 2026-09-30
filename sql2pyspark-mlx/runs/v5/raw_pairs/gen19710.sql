select e.level as employee_level, e.job_title as employee_job_title, m.salary as manager_salary, m.hire_date as manager_hire_date from employees e inner join employees m on e.manager_id = m.id;
