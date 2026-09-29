select id, hire_date, salary, is_remote, concat(email, ', ', job_title) email_job_title from employees where salary > 95000;
