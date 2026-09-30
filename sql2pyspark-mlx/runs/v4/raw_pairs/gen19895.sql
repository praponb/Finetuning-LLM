with cleaned as (
    select
        id,
        name,
        email,
        country,
        city,
        segment,
        signup_date,
        credit_limit,
        age,
        upper(segment) as segment_upper,
        coalesce(segment, 'NONE') as segment_filled
    from customers
    where segment is not null
)
select
    id,
    name,
    email,
    country,
    city,
    segment,
    signup_date,
    credit_limit,
    age,
    segment_upper,
    segment_filled,
    corr(credit_limit, age) over (partition by city) as corr_credit_limit_age,
    avg(age) over (partition by city, country order by signup_date, id rows between 5 preceding and current row) as age_avg_5
from cleaned;
