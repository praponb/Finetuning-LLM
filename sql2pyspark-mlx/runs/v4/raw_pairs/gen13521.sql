with enriched as (
    -- Running and moving aggregates
    select
        id,
        full_name,
        email,
        dept_id,
        manager_id,
        job_title,
        salary,
        hire_date,
        is_remote,
        level,
        lag(salary, 2) over (order by hire_date, id) as prev_salary,
        dense_rank() over (partition by job_title order by salary) as salary_dense_rank
    from employees
)
select
    id,
    full_name,
    dept_id,
    manager_id,
    job_title,
    salary,
    hire_date,
    level,
    prev_salary,
    case when job_title rlike '^(Manager|Engineer)$' then 'PRIORITY' when job_title rlike '^(Analyst|Director)$' then 'GROUP_A' else 'GROUP_B' end as job_title_zone
from enriched
