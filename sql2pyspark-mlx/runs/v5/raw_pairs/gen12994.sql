with cleaned as (
    -- Basic cleansing and defaults
    select
        *,
        coalesce(amount, 0) as amount_filled,
        substring(txn_type, 1, 4) as txn_type_prefix
    from transactions
    where merchant is not null
),
base as (
    -- Keep recent, valid rows
    select
        id,
        account_id,
        txn_type,
        amount,
        txn_time,
        merchant,
        is_flagged,
        amount_filled,
        txn_type_prefix,
        if(is_flagged, 'Yes', 'No') as is_flagged_label,
        coalesce(merchant, 'OTHER') as resolved_merchant
    from cleaned
    where txn_time >= timestamp '2023-08-12 16:00:00'
      and merchant is distinct from 'Grab'
),
latest as (
    -- QUALIFY-style window filter
    select *
    from base
    qualify row_number() over (partition by txn_type order by amount desc, id) = 1
),
with_gaps as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when txn_time - lag(txn_time) over (partition by txn_type order by txn_time, id) > interval '60' day then 1 else 0 end as new_group_flag
    from latest
),
sessions_transactions as (
    -- Running sum of the flag = session number
    select
        id,
        account_id,
        txn_type,
        amount,
        txn_time,
        merchant,
        amount_filled,
        txn_type_prefix,
        is_flagged_label,
        sum(new_group_flag) over (partition by txn_type order by txn_time, id rows between unbounded preceding and current row) as island_id
    from with_gaps
),
enriched as (
    -- Attach accounts attributes
    select e.*, a.status
    from sessions_transactions e
    inner join accounts a
        on e.account_id = a.id
)
-- Multi-level aggregation
select
    txn_type,
    round(avg(amount), 2) as avg_amount,
    max(amount_filled) as max_amount_filled,
    approx_percentile(amount, array(0.25, 0.5, 0.75)) as amount_quartiles
from enriched
group by 1
order by max_amount_filled desc nulls first;
