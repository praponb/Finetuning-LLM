with valid as (
    -- Keep recent, valid rows
    select
        id,
        email,
        country,
        city,
        segment,
        signup_date,
        is_active,
        credit_limit,
        age,
        case when city = 'New York' then 1 else 0 end as is_new_york,
        coalesce(name, 'Unknown') as name_filled
    from customers
    where segment = 'Corporate'
      and signup_date > to_date('2023-09-21')
)
-- Ranking and gap features
select
    id,
    country,
    city,
    segment,
    cume_dist() over (partition by country order by is_new_york) as is_new_york_cume_dist,
    ntile(3) over (partition by segment order by is_new_york desc, id) as is_new_york_bucket,
    lead(age, 1) over (partition by city order by signup_date, id) as next_age,
    ntile(4) over (order by credit_limit, id) as credit_limit_bucket,
    avg(credit_limit) over (partition by city, segment order by age, id rows between 3 preceding and 1 preceding) as credit_limit_avg_3_prior
from valid;
