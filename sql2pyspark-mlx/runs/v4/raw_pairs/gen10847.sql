with metrics as (
    select
        id,
        name,
        email,
        country,
        city,
        segment,
        signup_date,
        is_active,
        credit_limit,
        age,
        cume_dist() over (partition by segment order by age desc nulls first) as age_cume_dist,
        lead(age, 1) over (partition by segment order by age, id) as next_age
    from customers
)
select
    id,
    email,
    country,
    segment,
    signup_date,
    is_active,
    credit_limit,
    age,
    next_age
from metrics
order by id
