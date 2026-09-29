with ValidDepartments as (
    select *
    from departments
    where location in ('Berlin', 'London', 'New York') or dept_name in ('Engineering', 'Sales')
),
location_summary as (
    select location, case when budget >= 4113000 then 'high' when budget >= 884000 then 'mid' else 'low' end as budget_bucket, count(budget) as budget_count
    from ValidDepartments
    group by location, case when budget >= 4113000 then 'high' when budget >= 884000 then 'mid' else 'low' end
)
select location, budget_bucket, budget_count
from location_summary
where budget_count >= 10
order by budget_count desc, budget_bucket desc, location asc
limit 3;
