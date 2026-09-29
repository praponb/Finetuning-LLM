with recent_transactions as (
    select *
    from transactions
    where (txn_type = 'FEE' and merchant in ('7-Eleven', 'Shell')) or account_id in (8, 23)
),
transactions_stats as (
    select
        is_flagged,
        txn_type,
        sum(case when txn_type = 'FEE' then 1 else 0 end) as fee_count,
        sort_array(collect_set(merchant)) as merchant_list
    from recent_transactions
    group by is_flagged, txn_type
)
select
    is_flagged,
    txn_type,
    fee_count,
    merchant_list
from transactions_stats
order by txn_type
