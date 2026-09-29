with base as (
    -- Basic cleansing and defaults
    select
        *,
        upper(dept_name) as dept_name_upper,
        concat_ws('/', dept_name, location) as dept_name_location_key
    from departments
    where location = 'Berlin'
),
not_flagged as (
    -- Anti-join via a correlated subquery
    select s.*
    from base s
    where not exists (
            select 1
            from employees ch
            where ch.dept_id = s.id
              and ch.salary <= 141000
        )
),
ranked as (
    -- QUALIFY-style window filter
    select
        id,
        location,
        dept_name_upper
    from not_flagged
    qualify rank() over (partition by location order by budget desc) = 1
),
kept_keys as (
    -- Set difference on keys
    select id
    from ranked
    except distinct
    select id
    from departments
    where location not in ('Berlin', 'London', 'Bangkok')
),
verified as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from ranked s
    inner join kept_keys k on s.id = k.id
),
eligible as (
    -- Anti-join via a correlated subquery
    select s.*
    from verified s
    where not exists (
            select 1
            from employees c
            where c.dept_id = s.id
              and c.manager_id = 23
        )
),
filtered as (
    -- Basic cleansing and defaults
    select *, upper(location) as location_upper
    from eligible
    where location = 'Tokyo'
),
totals as (
    -- Aggregate with conditional metrics
    select
        location,
        count(case when location = 'Singapore' then 1 end) as singapore_count,
        count_if(location = 'Singapore') as singapore_events
    from filtered
    group by 1
)
select location, case when location rlike '^(New York|Chiang Mai)$' then 'GROUP_A' when location rlike '^(Bangkok|Singapore)$' then 'OTHER_ZONE' else 'PRIORITY' end as location_zone
from totals
order by location desc
