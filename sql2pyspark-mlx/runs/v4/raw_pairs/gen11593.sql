with filtered as (
    select
        *,
        concat_ws('-', location, dept_name) as location_dept_name_key,
        coalesce(location, 'DIRECT') as location_filled
    from departments
    where budget between 1789000 and 2967000
),
metrics as (
    select
        *,
        max(budget) over (partition by dept_name) as dept_name_max_budget,
        sum(budget) over (partition by dept_name, location order by budget asc, id asc rows between unbounded preceding and current row) as running_budget
    from filtered
),
base as (
    select
        *,
        round(budget, 1) as budget_rounded,
        case when budget >= 3995000 then 'High' when budget >= 763000 then 'Medium' else 'Low' end as budget_level
    from metrics
    where dept_name = 'Legal'
)
select
    dept_name,
    case when budget_rounded >= 87 then 'HIGH' else 'LOW' end as budget_rounded_tier,
    size(collect_set(location)) as num_locations,
    round(avg(running_budget), 2) as avg_running_budget,
    avg(budget) as avg_budget,
    round(avg(budget), 2) as avg_budget_2
from base
group by grouping sets ((dept_name, case when budget_rounded >= 87 then 'HIGH' else 'LOW' end), (dept_name))
having avg_running_budget >= 0;
