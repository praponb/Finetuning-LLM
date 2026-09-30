with ValidDepartments as (
    select *
    from departments
    where dept_name in ('Marketing', 'Finance')
),
dept_name_summary as (
    select dept_name, sum(case when dept_name = 'HR' then 1 else 0 end) as hr_count
    from ValidDepartments
    group by dept_name
)
select *
from dept_name_summary
order by hr_count desc;
