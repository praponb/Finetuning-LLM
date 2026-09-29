with ranked as (
    select
        id,
        dept_name,
        location,
        budget
    from (
        select *, row_number() over (partition by location, dept_name order by budget asc nulls last, id asc) as dedup_rank
        from departments
    ) ranked
    where dedup_rank = 1
)
select
    id,
    dept_name,
    location,
    budget
from ranked;
