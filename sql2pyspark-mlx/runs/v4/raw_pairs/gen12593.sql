with filtered as (
    -- Filter the raw rows and derive helper columns
    select
        *,
        round(age, 2) as age_rounded,
        coalesce(city, 'DIRECT') as city_filled
    from customers
    where signup_date >= date '2023-10-18'
      and age > 31
),
with_latest as (
    -- Latest matching order per row (LATERAL ... LIMIT 1)
    select s.*, latest.order_discount
    from filtered s
    join lateral (
            select c.discount as order_discount
            from orders c
            where c.customer_id = s.id
            order by c.discount desc, c.id desc
            limit 1
        ) latest on true
),
with_last as (
    -- Latest matching session per row (LATERAL ... LIMIT 1)
    select
        s.*,
        x.session_country,
        x.session_started_at
    from with_latest s
    left join lateral (
            select c.country as session_country, c.started_at as session_started_at
            from sessions c
            where c.customer_id = s.id
              and c.started_at <= s.signup_date
            order by c.started_at desc, c.id desc
            limit 1
        ) x on true
),
eligible as (
    -- Semi-join via a correlated subquery
    select s.*
    from with_last s
    where exists (
            select 1
            from orders x
            where x.customer_id = s.id
              and x.order_date >= s.signup_date
        )
)
-- Native SQL UNPIVOT (wide to long)
select *
from (
    select
        id,
        country,
        credit_limit,
        order_discount,
        age_rounded
    from eligible
) src
unpivot (
    reading for metric in (credit_limit as CREDIT_LIMIT_VAL, order_discount as ORDER_DISCOUNT, age_rounded as AGE_ROUNDED_VAL)
)
