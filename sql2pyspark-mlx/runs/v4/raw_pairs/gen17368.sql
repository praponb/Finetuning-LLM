with kept as (
    select s.*
    from departments s
    where exists (
            select 1
            from employees ch
            where ch.dept_id = s.id
              and ch.salary < 124000
        )
)
select
    id,
    dept_name,
    budget
from kept
order by id desc nulls last
