with filtered_departments as (
    select *
    from departments
    where location != 'New York'
),
Summary as (
    select dept_name, count(distinct budget) as unique_locations, sum(budget) as total_budget
    from filtered_departments
    group by dept_name
)
select dept_name, unique_locations, total_budget
from Summary
where unique_locations >= 6
order by dept_name desc, total_budget desc
