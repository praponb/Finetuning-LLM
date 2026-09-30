with base as (
    select
        *,
        lower(status) as status_lower,
        add_months(start_date, 1) as start_date_plus_months
    from subscriptions
    where start_date >= date '2024-01-07'
      and start_date > '2024-10-01'
),
with_dims as (
    select
        s.*,
        b.city,
        b.country
    from base s
    left join customers b
        on s.customer_id = b.id
),
deduped as (
    select *
    from (
        select *, row_number() over (partition by plan, country order by end_date, id) as rank_in_group
        from with_dims
    ) x
    where rank_in_group = 1
),
base_subscriptions as (
    select
        id,
        customer_id,
        start_date,
        end_date,
        monthly_fee,
        status_lower,
        start_date_plus_months,
        country,
        date_sub(end_date, 5) as end_date_minus_5d
    from deduped
    where end_date > date '2023-05-09'
      and status in ('EXPIRED', 'CANCELLED')
),
with_dims_subscriptions as (
    select
        e.*,
        b.country as customer_country,
        b.name
    from base_subscriptions e
    left join customers b
        on e.customer_id = b.id
)
select
    s.id,
    s.customer_id,
    s.start_date,
    s.monthly_fee,
    s.status_lower,
    s.start_date_plus_months,
    s.customer_country,
    case when customer_country rlike '^(US|DE)$' then 'GROUP_A' else 'CORE' end as customer_country_zone,
    (select max(a2.balance) from accounts a2 where a2.customer_id = s.customer_id) as max_account_balance
from with_dims_subscriptions s
order by start_date desc nulls first;
