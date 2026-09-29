with valid as (
    -- Keep recent, valid rows
    select
        *,
        ceil(budget) budget_whole,
        cast(budget as int) budget_int
    from departments
    where location is not null
      and budget >= 2156000
)
-- Native SQL PIVOT
select *
from (
    select
        location,
        dept_name,
        budget_whole
    from valid
) src
pivot (
    avg(budget_whole) as amount,
    count(1) as item_count
    for dept_name in ('Engineering', 'Sales')
);
