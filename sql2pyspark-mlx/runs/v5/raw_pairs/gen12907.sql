with scored as (
    select
        *,
        row_number() over w_recent as row_seq,
        row_number() over (order by txn_time desc, id) as row_seq_2,
        count(amount) over w_ordered as amount_count_90d
    from transactions
    window w_recent as (partition by txn_type order by txn_time desc, id), w_ordered as (partition by txn_type, account_id order by cast(txn_time as bigint) range between 7776000 preceding and current row)
)
select
    id,
    amount,
    row_seq,
    amount_count_90d
from scored;
