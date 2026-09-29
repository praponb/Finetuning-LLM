with with_windows as (
    select
        id,
        account_id,
        merchant,
        is_flagged,
        avg(amount) over (partition by merchant order by amount, id rows between 5 preceding and current row) as amount_avg_5,
        last_value(amount, true) over (partition by txn_type order by amount, id rows between unbounded preceding and current row) as last_known_amount,
        row_number() over (partition by merchant order by txn_time, id) as event_seq,
        dense_rank() over w_hist as amount_dense_rank,
        lag(amount, 1) over (partition by txn_type, merchant order by txn_time, id) as prev_amount
    from transactions
    window w_hist as (partition by account_id order by amount desc nulls last)
)
select
    id,
    merchant,
    amount_avg_5,
    last_known_amount,
    amount_dense_rank,
    prev_amount,
    case when merchant rlike '^(Shell|Amazon)$' then 'EXTENDED' else 'GROUP_B' end as merchant_zone
from with_windows
order by id desc, last_known_amount
limit 10;
