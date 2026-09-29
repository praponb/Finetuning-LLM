select dept_id from employees where length(email) < 11 except select dept_id from employees where job_title is null
