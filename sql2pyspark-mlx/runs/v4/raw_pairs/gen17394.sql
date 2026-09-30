with filtered as (
    select
        *,
        substring(location, 1, 2) as location_prefix,
        coalesce(dept_name, 'OTHER') as resolved_dept_name
    from departments
    where location is not null
      and location in ('New York', 'Singapore', 'Tokyo')
),
latest as (
    select *, row_number() over (partition by location order by budget desc nulls first, id) as latest_rank
    from filtered
    qualify latest_rank = 1
),
eligible as (
    select s.*
    from latest s
    where not exists (
            select 1
            from employees x
            where x.dept_id = s.id
              and x.job_title = 'Director'
        )
),
metrics as (
    select
        *,
        count(budget) over w_hist as location_count_budget,
        first_value(location) ignore nulls over (order by latest_rank, id rows between unbounded preceding and current row) as first_known_location
    from eligible
    window w_hist as (partition by location)
)
select
    id,
    dept_name,
    location,
    budget,
    location_prefix,
    resolved_dept_name,
    latest_rank,
    first_known_location,
    case when location rlike '^(Singapore|Tokyo)$' then 'GROUP_A' when location rlike '^(Bangkok|Chiang Mai)$' then 'EXTENDED' else 'PRIORITY' end as location_zone
from metrics
order by budget desc nulls first
