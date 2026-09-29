with filtered as (
    -- Filter the raw rows and derive helper columns
    select
        *,
        coalesce(monthly_fee, 0) as monthly_fee_filled,
        substring(status, 1, 2) as status_prefix
    from subscriptions
    where end_date >= date '2024-03-01'
      and customer_id is not null
      and plan in ('basic', 'standard', 'enterprise')
),
with_windows as (
    -- Window metrics over several different partitions
    select
        id,
        customer_id,
        plan,
        start_date,
        end_date,
        monthly_fee,
        status,
        monthly_fee_filled,
        status_prefix,
        rank() over (order by monthly_fee nulls last) as monthly_fee_rank,
        sum(monthly_fee_filled) over (order by start_date, id rows between unbounded preceding and current row) as running_monthly_fee_filled,
        min(monthly_fee) over (partition by plan, customer_id) as plan_min_monthly_fee
    from filtered
),
enriched as (
    -- Ranking and gap features
    select
        *,
        ntile(5) over (partition by plan order by monthly_fee_filled desc, id) as monthly_fee_filled_bucket,
        avg(monthly_fee_rank) over (partition by customer_id, plan order by running_monthly_fee_filled, id rows between 6 preceding and current row) as monthly_fee_rank_avg_6
    from with_windows
),
latest as (
    -- Keep one row per key (latest first)
    select *
    from enriched
    qualify row_number() over (partition by customer_id order by monthly_fee desc nulls last, id) = 1
),
deduped as (
    -- Keep one row per key (latest first)
    select *
    from (
        select *, row_number() over (partition by plan order by end_date desc, id) as row_num
        from latest
    ) t
    where row_num = 1
),
ranked as (
    -- Keep one row per key (latest first)
    select *, rank() over (partition by plan order by monthly_fee_filled nulls first) as latest_rank
    from deduped
    qualify latest_rank = 1
)
select
    s.id,
    s.customer_id,
    s.plan,
    s.end_date,
    s.status,
    s.monthly_fee_filled,
    s.status_prefix,
    s.running_monthly_fee_filled,
    s.monthly_fee_rank_avg_6,
    (select avg(o2.shipping_fee) from orders o2 where o2.customer_id = s.customer_id) as avg_order_shipping_fee
from ranked s;
