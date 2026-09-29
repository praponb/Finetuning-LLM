with latest as (
    -- QUALIFY-style window filter
    select
        id,
        name,
        email,
        city,
        signup_date,
        is_active,
        age
    from (
        select *, row_number() over (partition by country order by age, id) as latest_rank
        from customers
    ) x
    where latest_rank = 1
)
select
    id,
    city,
    signup_date,
    is_active,
    age
from latest
order by id nulls last;
