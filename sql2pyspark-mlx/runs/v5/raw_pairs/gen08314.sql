with filtered as (
    select
        id,
        account_id,
        txn_type,
        amount,
        txn_time,
        is_flagged,
        lower(txn_type) as txn_type_lower,
        coalesce(merchant, 'N/A') as merchant_filled
    from transactions
    where txn_time >= to_date('2023-06-15')
      and merchant is distinct from 'Lazada'
),
ranked as (
    select
        id,
        account_id,
        amount,
        txn_time,
        is_flagged,
        txn_type_lower
    from filtered
    qualify row_number() over (partition by txn_type order by amount, id) = 1
),
enriched as (
    select
        *,
        ntile(10) over (order by amount desc, id) as amount_bucket,
        round(amount / sum(amount) over (partition by account_id), 4) as amount_share,
        lead(amount, 1) over (partition by account_id order by amount, id) as next_amount
    from ranked
),
latest as (
    select
        id,
        account_id,
        txn_time,
        is_flagged,
        txn_type_lower,
        amount_bucket
    from (
        select *, row_number() over (partition by account_id order by amount_bucket desc, id) as rn
        from enriched
    ) x
    where rn = 1
),
with_windows as (
    select
        *,
        last_value(amount_bucket, true) over (order by txn_time, id rows between unbounded preceding and current row) as last_known_amount_bucket,
        lag(amount_bucket, 2) over (partition by account_id order by txn_time, id) as prev_amount_bucket,
        cume_dist() over (partition by account_id order by amount_bucket desc nulls last) as amount_bucket_cume_dist
    from latest
),
deduped as (
    select
        id,
        account_id,
        txn_time,
        is_flagged,
        txn_type_lower,
        amount_bucket,
        last_known_amount_bucket,
        prev_amount_bucket
    from with_windows
    qualify row_number() over (partition by account_id order by last_known_amount_bucket desc nulls first, id) = 1
),
summary as (
    select
        case when last_known_amount_bucket >= 8 then 'HIGH' else 'LOW' end as last_known_amount_bucket_tier,
        min(last_known_amount_bucket) as min_last_known_amount_bucket,
        max(txn_time) as last_txn_time,
        round(avg(amount_bucket), 2) as avg_amount_bucket
    from deduped
    group by case when last_known_amount_bucket >= 8 then 'HIGH' else 'LOW' end
)
select
    last_known_amount_bucket_tier,
    min_last_known_amount_bucket,
    avg_amount_bucket
from summary
order by last_known_amount_bucket_tier;
