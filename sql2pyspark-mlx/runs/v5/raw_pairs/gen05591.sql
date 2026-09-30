with not_flagged as (
    select s.*
    from departments s
    where not exists (
            select 1
            from employees c
            where c.dept_id = s.id
              and c.full_name like '%MILLE%'
        )
),
ranked as (
    select *
    from not_flagged
    qualify row_number() over (partition by location, dept_name order by budget desc, id) = 1
),
deduped as (
    select id, location
    from ranked
    qualify row_number() over (partition by location, dept_name order by budget desc, id) = 1
),
with_last as (
    select s.*, x.job_title
    from deduped s
    left join lateral (
            select c.job_title
            from employees c
            where c.dept_id = s.id
            order by c.salary desc, c.id desc
            limit 1
        ) x on true
),
eligible as (
    select s.*
    from with_last s
    where not exists (
            select 1
            from employees x
            where x.dept_id = s.id
              and x.is_remote
        )
)
select id
from eligible
order by id desc nulls last
