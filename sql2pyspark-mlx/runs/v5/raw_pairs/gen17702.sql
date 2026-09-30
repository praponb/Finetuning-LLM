with filtered_departments as (
    select *
    from departments
    where dept_name in ('Legal', 'Engineering')
),
agg as (
    select location, count(budget) as budget_count
    from filtered_departments
    group by location
)
select *
from agg
order by location
