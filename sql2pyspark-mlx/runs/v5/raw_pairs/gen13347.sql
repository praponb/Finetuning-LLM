select * from (select job_title, level, salary from employees)
pivot (max(salary) for level in ('L3', 'L2', 'L1', 'L4'))
order by job_title;
