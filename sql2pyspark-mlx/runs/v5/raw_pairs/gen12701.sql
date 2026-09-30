select *
from (
    select
        manager_id,
        dept_id,
        job_title,
        salary
    from employees
) src
pivot (
    avg(salary)
    for job_title in ('Director', 'Manager')
);
