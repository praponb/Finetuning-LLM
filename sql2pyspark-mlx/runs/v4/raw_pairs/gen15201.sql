with valid as (
    -- Keep recent, valid rows
    select
        *,
        initcap(trim(country)) country_title,
        round(credit_limit / age, 2) credit_limit_per_age,
        coalesce(city, 'N/A') city_filled
    from customers
    where signup_date >= date '2023-11-13'
      and segment is not null
      and not is_active
),
with_last as (
    -- Latest matching account per row (LATERAL ... LIMIT 1)
    select
        s.*,
        x.balance,
        x.account_opened_date
    from valid s
    left join lateral (
            select c.balance, c.opened_date as account_opened_date
            from accounts c
            where c.customer_id = s.id
            order by c.balance desc, c.id desc
            limit 1
        ) x on true
),
cleaned as (
    -- Basic cleansing and defaults
    select
        *,
        lower(country) country_lower,
        coalesce(city, 'OTHER') city_clean
    from with_last
    where signup_date >= date '2023-07-05'
      and city is not null
      and email not like '%@gmail.com'
)
-- Native SQL UNPIVOT (wide to long)
select *
from (
    select
        id,
        city,
        credit_limit_per_age,
        credit_limit,
        balance
    from cleaned
) src
unpivot include nulls (
    metric_value for metric_name in (credit_limit_per_age, credit_limit as CREDIT_LIMIT_VAL, balance as BALANCE_VAL)
)
