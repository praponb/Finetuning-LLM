select id, job_title, salary, row_number() over (partition by job_title order by salary, id) as row_num from employees
