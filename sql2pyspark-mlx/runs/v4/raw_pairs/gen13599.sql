with filtered_employees as (
    select *
    from employees
    where not ((dept_id in (2, 35, 36) and dept_id = 34) or lower(email) like '%iam%')
),
job_title_summary as (
    select job_title, case when salary >= 120000 then 'high' when salary >= 60000 then 'mid' else 'low' end as salary_bucket, count(distinct salary) as unique_depts
    from filtered_employees
    group by job_title, case when salary >= 120000 then 'high' when salary >= 60000 then 'mid' else 'low' end
)
select *
from job_title_summary
order by unique_depts
