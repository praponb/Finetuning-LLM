with valid as (
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
        round(credit_limit / age, 2) as credit_limit_per_age
    from customers
    where signup_date >= to_date('2023-03-29')
      and segment is distinct from 'Corporate'
)
select *
from (
    select
        id,
        segment,
        credit_limit_per_age,
        credit_limit
    from valid
) src
unpivot include nulls (
    value for metric in (credit_limit_per_age, credit_limit)
) long_rows;
