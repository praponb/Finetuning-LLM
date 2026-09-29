select * from (select level, job_title, salary from employees) src pivot (avg(salary) for job_title in ('Manager' as manager, 'Designer' as designer));
