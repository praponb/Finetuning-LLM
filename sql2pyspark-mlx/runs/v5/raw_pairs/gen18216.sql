with kept_keys as (
    -- Set difference on keys
    select txn_type
    from transactions
    except distinct
    select txn_type
    from transactions
    where amount between 1300 and 2500
),
kept_rows as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from transactions s
    inner join kept_keys k on s.txn_type = k.txn_type
),
base as (
    -- Filter the raw rows and derive helper columns
    select *, round(amount, 0) as amount_rounded
    from kept_rows
    where merchant is distinct from 'Amazon'
),
matched_keys as (
    -- Set intersection on keys
    select txn_type, merchant
    from base
    intersect
    select txn_type, merchant
    from transactions
    where txn_type not in ('REFUND', 'CREDIT', 'DEBIT')
),
verified as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from base s
    inner join matched_keys k on s.txn_type = k.txn_type and s.merchant = k.merchant
),
base_transactions as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        txn_type,
        amount,
        merchant,
        is_flagged,
        case when txn_type in ('DEBIT', 'FEE') then 'Priority' else 'Other' end as txn_type_group
    from verified
    where txn_time >= to_timestamp('2023-09-13 16:00:00')
)
-- Aggregate with conditional metrics
select
    case when amount >= 4500 then 'LARGE' else 'SMALL' end as amount_tier,
    sum(case when txn_type = 'CREDIT' then amount else 0 end) as credit_amount,
    approx_percentile(amount, array(0.25, 0.5, 0.75)) as amount_quartiles,
    avg(amount) as avg_amount,
    count_if(merchant = 'Shell') as shell_events,
    size(collect_set(merchant)) as num_merchants,
    size(collect_set(txn_type)) as num_txn_types
from base_transactions
group by amount_tier
order by num_merchants desc nulls last
