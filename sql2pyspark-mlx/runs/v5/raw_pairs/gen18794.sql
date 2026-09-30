with latest as (
    select
        id,
        dept_name,
        location
    from (
        select *, rank() over (partition by location, dept_name order by budget desc) as latest_rank
        from departments
    ) ranked
    where latest_rank = 1
),
eligible as (
    select s.*
    from latest s
    where exists (
            select 1
            from employees c
            where c.dept_id = s.id
              and c.hire_date between '2023-07-01' and '2025-02-17'
        )
),
eligible_departments as (
    select s.*
    from eligible s
    where not exists (
            select 1
            from employees c
            where c.dept_id = s.id
              and c.email like '%@gmail.com'
        )
),
not_flagged as (
    select s.*
    from eligible_departments s
    where not exists (
            select 1
            from employees x
            where x.dept_id = s.id
              and x.hire_date >= '2025-04-01'
        )
),
base as (
    select *, lower(location) location_lower
    from not_flagged
    where dept_name is distinct from 'Sales'
      and dept_name in ('Sales', 'HR', 'Marketing')
),
cleaned as (
    select *, length(location) location_length
    from base
    where location not in ('Bangkok', 'Berlin', 'New York')
)
select
    case when location_length >= 81 then 'LARGE' else 'SMALL' end as location_length_tier,
    dept_name,
    count_if(dept_name = 'Sales') as sales_events,
    percentile_approx(location_length, 0.95) as location_length_p95,
    count(distinct location) as distinct_locations,
    grouping_id() as grouping_id
from cleaned
group by rollup (case when location_length >= 81 then 'LARGE' else 'SMALL' end, dept_name);
