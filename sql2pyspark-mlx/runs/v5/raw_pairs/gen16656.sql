with base as (
    -- Keep recent, valid rows
    select
        id,
        account_id,
        txn_type,
        amount,
        txn_time,
        merchant,
        is_flagged,
        substring(txn_type, 1, 2) txn_type_prefix,
        trim(txn_type) clean_txn_type
    from transactions
    where txn_time > date '2023-11-09'
      and merchant is not null
),
remaining_keys as (
    -- Set intersection on keys
    select id
    from base
    intersect all
    select id
    from transactions
    where is_flagged
),
kept_rows as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from base s
    join remaining_keys k on s.id = k.id
),
metrics as (
    -- Window metrics over several different partitions
    select
        *,
        sum(amount) over (partition by merchant) merchant_sum_amount,
        cume_dist() over (order by amount nulls last) amount_cume_dist,
        count(amount) over (partition by txn_type order by cast(txn_time as bigint) range between 604800 preceding and current row) amount_count_7d
    from kept_rows
),
enriched as (
    -- Attach accounts attributes
    select
        e.*,
        d.account_type,
        d.balance
    from metrics e
    left join accounts d
        on e.account_id = d.id
)
-- Aggregate with conditional metrics
select
    case when amount_cume_dist >= 0.81 then 'PREMIUM' else 'STANDARD' end as amount_cume_dist_tier,
    case when amount >= 3100 then 'PREMIUM' else 'STANDARD' end as amount_tier,
    count(*) as num_rows,
    count(distinct account_id) as distinct_account_ids,
    count(*) filter (where txn_type = 'REFUND') as refund_rows,
    grouping(case when amount_cume_dist >= 0.81 then 'PREMIUM' else 'STANDARD' end) as grouping_id
from enriched
group by grouping sets ((case when amount_cume_dist >= 0.81 then 'PREMIUM' else 'STANDARD' end, case when amount >= 3100 then 'PREMIUM' else 'STANDARD' end), (case when amount_cume_dist >= 0.81 then 'PREMIUM' else 'STANDARD' end), ())
having count(*) >= 1
