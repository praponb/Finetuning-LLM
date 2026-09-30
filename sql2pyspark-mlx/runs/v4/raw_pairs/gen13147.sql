with cleaned as (
    select
        *,
        add_months(signup_date, 6) as signup_date_plus_months,
        date_trunc('month', signup_date) as signup_date_period,
        coalesce(segment, 'OTHER') as segment_filled
    from customers
    where signup_date >= to_date('2023-12-19')
      and segment is not null
)
select
    *,
    lead(credit_limit, 1) over (partition by country order by signup_date, id) as next_credit_limit,
    corr(age, credit_limit) over (partition by country, segment) as corr_age_credit_limit,
    row_number() over (partition by city order by age, id) as seq_no,
    max(age) over (partition by country) as country_max_age
from cleaned
