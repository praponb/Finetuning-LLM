with cleaned as (
    -- Keep recent, valid rows
    select
        *,
        case when monthly_fee >= 243 then 'High' when monthly_fee >= 103 then 'Medium' else 'Low' end as monthly_fee_tier,
        case when plan = 'premium' then 1 else 0 end as is_premium
    from subscriptions
    where customer_id in (5, 26, 29)
),
deduped as (
    -- QUALIFY-style window filter
    select *
    from (
        select *, row_number() over (partition by customer_id order by is_premium desc nulls first, id) as dedup_rank
        from cleaned
    ) ranked
    where dedup_rank = 1
),
scored as (
    -- Ranking and gap features
    select
        id,
        customer_id,
        plan,
        start_date,
        end_date,
        monthly_fee,
        status,
        monthly_fee_tier,
        is_premium,
        dedup_rank,
        dense_rank() over (partition by plan order by monthly_fee nulls first) as monthly_fee_dense_rank,
        row_number() over (order by is_premium, id) as seq_no,
        ntile(3) over (partition by status order by monthly_fee desc, id) as monthly_fee_bucket,
        dense_rank() over (order by is_premium desc nulls first) as is_premium_dense_rank,
        round(monthly_fee / sum(monthly_fee) over (partition by plan), 4) as monthly_fee_share
    from deduped
),
enriched as (
    -- Window metrics over several different partitions
    select
        id,
        customer_id,
        plan,
        start_date,
        end_date,
        monthly_fee,
        status,
        monthly_fee_tier,
        is_premium,
        dedup_rank,
        monthly_fee_dense_rank,
        seq_no,
        monthly_fee_bucket,
        is_premium_dense_rank,
        monthly_fee_share,
        row_number() over (partition by status order by is_premium_dense_rank, id) as row_seq,
        round(is_premium_dense_rank / sum(is_premium_dense_rank) over (partition by plan), 4) as is_premium_dense_rank_share,
        ntile(4) over (order by monthly_fee, id) as monthly_fee_bucket_2
    from scored
),
deduped_subscriptions as (
    -- Keep one row per key (latest first)
    select *, row_number() over (partition by plan order by is_premium_dense_rank desc nulls last, id) as rank_in_group
    from enriched
    qualify rank_in_group = 1
),
attributed as (
    -- Non-equi join on a validity window
    select
        s.*,
        sub.id as subscription_id,
        sub.plan as subscription_plan,
        sub.monthly_fee as subscription_monthly_fee
    from deduped_subscriptions s
    left join subscriptions sub
        on s.customer_id = sub.customer_id
       and s.start_date >= sub.start_date
       and s.start_date < sub.end_date + interval '30' day
)
-- Native SQL UNPIVOT (wide to long)
select *
from (
    select
        id,
        subscription_id,
        plan,
        monthly_fee,
        monthly_fee_share,
        is_premium_dense_rank_share,
        subscription_monthly_fee
    from attributed
) src
unpivot include nulls (
    reading for metric in (monthly_fee as MONTHLY_FEE_VAL, monthly_fee_share as MONTHLY_FEE_SHARE, is_premium_dense_rank_share as IS_PREMIUM_DENSE_RANK_SHARE, subscription_monthly_fee)
)
