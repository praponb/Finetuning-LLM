with filtered as (
    -- Basic cleansing and defaults
    select
        *,
        if(is_flagged, 'Yes', 'No') as is_flagged_label,
        trim(merchant) as clean_merchant
    from transactions
    where txn_time >= to_date('2024-04-07')
),
scored as (
    -- Ranking and gap features
    select
        id,
        account_id,
        txn_type,
        amount,
        txn_time,
        merchant,
        is_flagged,
        is_flagged_label,
        clean_merchant,
        max(amount) over w_running as amount_max_90d,
        dense_rank() over (partition by txn_type order by amount desc nulls first) as amount_dense_rank,
        count(amount) over (partition by txn_type) as txn_type_count_amount,
        lag(amount, 1) over (partition by account_id, txn_type order by amount, id) as prev_amount,
        avg(amount) over (partition by account_id order by amount, id rows between 3 preceding and current row) as amount_avg_3
    from filtered
    window w_running as (partition by merchant order by cast(txn_time as bigint) range between 7776000 preceding and current row)
),
joined as (
    -- Attach accounts attributes
    select e.*, p.status
    from scored e
    join accounts p
        on e.account_id = p.id
)
select
    s.id,
    s.account_id,
    s.amount,
    s.txn_time,
    s.merchant,
    s.is_flagged_label,
    s.clean_merchant,
    s.amount_dense_rank,
    s.txn_type_count_amount,
    case when merchant rlike '^(Amazon|Lazada)$' then 'GROUP_B' when merchant rlike '^(Grab|7-Eleven)$' then 'EXTENDED' else 'PRIORITY' end as merchant_zone,
    (select avg(t2.amount) from transactions t2 where t2.account_id = s.account_id) as avg_transaction_amount
from joined s
order by amount_dense_rank nulls first
