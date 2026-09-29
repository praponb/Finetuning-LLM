with filtered as (
    -- Keep recent, valid rows
    select
        id,
        customer_id,
        plan,
        start_date,
        end_date,
        monthly_fee,
        case when plan = 'basic' then 1 else 0 end as is_basic
    from subscriptions
    where status = 'ACTIVE'
),
latest as (
    -- Deduplicate with ROW_NUMBER
    select *, row_number() over (partition by customer_id, plan order by is_basic desc nulls last, id asc) as rank_in_group
    from filtered
    qualify rank_in_group = 1
),
matched as (
    -- Non-equi join on a validity window
    select
        s.*,
        sub.id as subscription_id,
        sub.plan as subscription_plan,
        sub.monthly_fee as subscription_monthly_fee
    from latest s
    left join subscriptions sub
        on s.customer_id = sub.customer_id
       and s.end_date between sub.start_date and sub.end_date
),
attributed as (
    -- Non-equi join on a validity window
    select
        src.*,
        sub.id as subscription_id_2,
        sub.plan as subscription_plan_2,
        sub.monthly_fee as subscription_monthly_fee_2
    from matched src
    left join subscriptions sub
        on src.customer_id = sub.customer_id
       and src.start_date >= sub.start_date
       and src.start_date <= date_add(sub.end_date, 7)
),
matched_subscriptions as (
    -- Non-equi join on a validity window
    select
        s.*,
        sub.id as subscription_id_3,
        sub.plan as subscription_plan_3,
        sub.monthly_fee as subscription_monthly_fee_3
    from attributed s
    left join subscriptions sub
        on s.customer_id = sub.customer_id
       and s.start_date between sub.start_date and sub.end_date
),
filtered_subscriptions as (
    -- Basic cleansing and defaults
    select
        id,
        customer_id,
        plan,
        start_date,
        monthly_fee,
        is_basic,
        rank_in_group,
        subscription_id,
        subscription_plan,
        subscription_monthly_fee,
        subscription_id_2,
        subscription_plan_2,
        subscription_id_3,
        subscription_monthly_fee_3,
        cast(monthly_fee as int) as monthly_fee_int,
        coalesce(subscription_plan_3, 'N/A') as subscription_plan_3_filled,
        coalesce(subscription_plan_2, 'UNKNOWN') as resolved_subscription_plan_2
    from matched_subscriptions
    where start_date >= date '2023-05-20'
      and subscription_plan_3 is not null
),
deduped as (
    -- Deduplicate with ROW_NUMBER
    select *
    from (
        select *, row_number() over (partition by customer_id order by subscription_id desc nulls first, id asc, subscription_id asc, subscription_id_2 asc, subscription_id_3 asc) as row_num
        from filtered_subscriptions
    ) t
    where row_num = 1
),
grouped as (
    -- Aggregate with conditional metrics
    select
        case when is_basic >= 62 then 'HIGH' else 'LOW' end as is_basic_tier,
        date_trunc('year', start_date) as year_start,
        subscription_plan_2,
        avg(monthly_fee) as avg_monthly_fee,
        count_if(subscription_plan = 'basic') as basic_events,
        count(case when plan = 'standard' then 1 end) as standard_count
    from deduped
    group by grouping sets ((case when is_basic >= 62 then 'HIGH' else 'LOW' end, date_trunc('year', start_date), subscription_plan_2), (case when is_basic >= 62 then 'HIGH' else 'LOW' end, date_trunc('year', start_date)), ())
)
select
    is_basic_tier,
    subscription_plan_2,
    basic_events
from grouped
order by basic_events asc nulls last;
