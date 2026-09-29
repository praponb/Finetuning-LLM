with with_windows as (
    select
        *,
        lead(age, 2, 0) over (partition by city, country order by signup_date, id) as next_age,
        percent_rank() over (partition by country order by age nulls first) as age_percent_rank,
        cume_dist() over (order by age nulls first) as age_cume_dist,
        row_number() over (partition by city order by age, id) as seq_no,
        avg(credit_limit) over (order by credit_limit, id rows between 3 preceding and 1 preceding) as credit_limit_avg_3_prior
    from customers
)
select
    id,
    name,
    email,
    country,
    signup_date,
    is_active,
    credit_limit,
    next_age,
    age_percent_rank
from with_windows
order by next_age desc, id desc
limit 10;
