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
        cume_dist() over (partition by segment order by credit_limit desc nulls first) credit_limit_cume_dist,
        row_number() over (partition by segment order by signup_date desc, id asc) row_seq
    from customers
)
select
    id,
    name,
    email,
    country,
    segment,
    credit_limit,
    age,
    credit_limit_cume_dist
from metrics
order by segment desc
