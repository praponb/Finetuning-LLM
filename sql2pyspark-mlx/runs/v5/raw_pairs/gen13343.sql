with valid as (
    select
        *,
        initcap(trim(dept_name)) as dept_name_title,
        coalesce(location, 'N/A') as location_filled
    from departments
    where budget between 1241000 and 2646000
)
select
    case when budget >= 4054000 then 'LARGE' else 'SMALL' end as budget_tier,
    dept_name,
    sum(budget) filter (where location = 'Chiang Mai') as chiang_mai_total_budget,
    count_if(dept_name = 'HR') as hr_events
from valid
group by grouping sets ((case when budget >= 4054000 then 'LARGE' else 'SMALL' end, dept_name), (case when budget >= 4054000 then 'LARGE' else 'SMALL' end), ())
