with valid as (
    -- Filter the raw rows and derive helper columns
    select
        *,
        concat(dept_name, '/', location) as dept_name_location,
        coalesce(location, 'NONE') as resolved_location
    from departments
    where location = 'Tokyo'
)
-- Grouped metrics
select
    dept_name,
    location,
    avg(budget) filter (where location = 'Tokyo') as tokyo_avg_budget,
    avg(budget) as avg_budget,
    count(case when location = 'Tokyo' then 1 end) as tokyo_count,
    sum(case when dept_name = 'Marketing' then budget else 0 end) as marketing_budget
from valid
group by 1, 2;
