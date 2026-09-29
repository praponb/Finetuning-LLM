with filtered as (
    -- Keep recent, valid rows
    select
        *,
        initcap(trim(location)) as location_title,
        substring(dept_name, 1, 2) as dept_name_prefix
    from departments
    where location is not null
)
select location
from filtered
intersect all
select location
from departments
where location in ('New York', 'Berlin', 'London');
