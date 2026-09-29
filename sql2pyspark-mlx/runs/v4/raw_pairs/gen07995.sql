with kept_keys as (
    select id
    from departments
    except distinct
    select id
    from departments
    where dept_name is not null
),
verified as (
    select s.*
    from departments s
    join kept_keys k on s.id = k.id
)
select
    id,
    location,
    budget
from verified;
