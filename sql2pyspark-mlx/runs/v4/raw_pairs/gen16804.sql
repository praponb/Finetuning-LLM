select *
from (
    select
        dept_id,
        job_title,
        salary
    from employees
) src
pivot (
    max(salary) as amount,
    count(1) as n
    for job_title in ('Designer' as designer, 'Engineer' as engineer)
);
