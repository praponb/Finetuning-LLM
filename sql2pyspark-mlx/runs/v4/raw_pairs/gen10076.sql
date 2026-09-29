with matched_keys as (
    select id, location
    from departments
    except
    select id, location
    from departments
    where location is not null
),
reconciled as (
    select s.*
    from departments s
    inner join matched_keys k on s.id = k.id and s.location = k.location
)
select
    location,
    case when budget >= 3251000 then 'PREMIUM' else 'STANDARD' end as budget_tier,
    dept_name,
    count(*) as row_count,
    avg(budget) as avg_budget,
    round(sum(budget) / nullif(count(case when location = 'New York' then 1 end), 0), 2) as budget_per_new_york,
    sum(case when dept_name = 'Marketing' then budget else 0 end) as marketing_budget,
    count(case when location = 'London' then 1 end) as london_count,
    sum(budget) filter (where location = 'Singapore') as singapore_total_budget
from reconciled
group by rollup (location, case when budget >= 3251000 then 'PREMIUM' else 'STANDARD' end, dept_name)
having round(sum(budget) / nullif(count(case when location = 'New York' then 1 end), 0), 2) > 0
order by row_count
