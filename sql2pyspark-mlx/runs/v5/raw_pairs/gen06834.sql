with filtered as (
    select
        *,
        ceil(budget) budget_whole,
        concat(dept_name, ' - ', location) dept_name_location,
        coalesce(location, 'DIRECT') location_filled
    from departments
    where dept_name is not null
),
enriched as (
    select
        id,
        location,
        dept_name_location,
        location_filled,
        ntile(5) over (partition by location order by budget_whole desc, id) budget_whole_bucket,
        ntile(10) over (partition by location order by budget desc, id) budget_bucket,
        rank() over (partition by dept_name, location order by budget desc nulls last) budget_rank
    from filtered
),
with_latest as (
    select
        s.id,
        s.location,
        s.dept_name_location,
        x.employee_level
    from enriched s
    left join lateral (
            select c.level as employee_level
            from employees c
            where c.dept_id = s.id
            order by c.salary desc, c.id desc
            limit 1
        ) x on true
),
filtered_departments as (
    select
        *,
        substring(location, 1, 3) location_prefix,
        coalesce(location, 'UNKNOWN') location_filled
    from with_latest
    where employee_level is not null
      and location is distinct from 'Berlin'
),
kept as (
    select s.*
    from filtered_departments s
    where not exists (
            select 1
            from employees x
            where x.dept_id = s.id
              and x.full_name like 'Iv%'
        )
)
select
    id,
    location,
    dept_name_location,
    employee_level,
    case when location rlike '^(New York|Bangkok)$' then 'OTHER_ZONE' else 'GROUP_B' end as location_zone
from kept
order by location_zone nulls first, employee_level;
