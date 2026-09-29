with deduped as (
    -- QUALIFY-style window filter
    select *, row_number() over (partition by location order by budget, id) as latest_rank
    from departments
    qualify latest_rank = 1
),
ranked as (
    -- QUALIFY-style window filter
    select *, row_number() over (partition by location order by latest_rank desc, id) as rank_in_group
    from deduped
    qualify rank_in_group = 1
),
cleaned as (
    -- Filter the raw rows and derive helper columns
    select *, lower(dept_name) dept_name_lower
    from ranked
    where location is not null
)
select
    id,
    location,
    budget,
    latest_rank,
    rank_in_group
from cleaned
order by rank_in_group;
