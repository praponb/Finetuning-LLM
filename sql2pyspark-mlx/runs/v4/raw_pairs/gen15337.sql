select id, job_title, manager_id, length(email) as email_length from employees where salary > 74000 and level not in ('L3', 'L2', 'L4') order by 1, manager_id desc
