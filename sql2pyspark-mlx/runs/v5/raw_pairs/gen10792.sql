with cleaned as (
    -- Basic cleansing and defaults
    select *, date_add(end_date, 12) as end_date_plus_12d
    from subscriptions
    where start_date > date '2023-03-13'
      and status not in ('CANCELLED', 'PAUSED', 'ACTIVE')
),
matched as (
    -- Non-equi join on a validity window
    select
        a.*,
        sub.id as subscription_id,
        sub.plan as subscription_plan,
        sub.monthly_fee as subscription_monthly_fee
    from cleaned a
    left join subscriptions sub
        on a.customer_id = sub.customer_id
       and a.end_date_plus_12d >= sub.start_date
       and a.end_date_plus_12d < sub.end_date + interval '30' day
),
enriched as (
    -- Window metrics over several different partitions
    select
        id,
        customer_id,
        end_date,
        status,
        end_date_plus_12d,
        subscription_id,
        cume_dist() over (order by subscription_id nulls first) as subscription_id_cume_dist,
        cume_dist() over (partition by plan order by subscription_id) as subscription_id_cume_dist_2
    from matched
),
with_window as (
    -- Non-equi join on a validity window
    select
        a.*,
        sub.id as subscription_id_2,
        sub.plan,
        sub.monthly_fee
    from enriched a
    left join subscriptions sub
        on a.customer_id = sub.customer_id
       and a.end_date_plus_12d >= sub.start_date
       and a.end_date_plus_12d <= date_add(sub.end_date, 7)
)
-- Join a dimension and aggregate
select
    dim.country,
    dim.segment,
    count(*) as cnt,
    max(e.subscription_id_2) as max_subscription_id_2
from with_window e
inner join customers dim on e.customer_id = dim.id
group by dim.country, dim.segment;
