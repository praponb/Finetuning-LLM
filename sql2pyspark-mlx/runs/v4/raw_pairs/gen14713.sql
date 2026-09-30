with valid as (
    select
        id,
        account_id,
        txn_type,
        amount,
        txn_time,
        merchant,
        concat_ws(',', merchant, txn_type) as merchant_txn_type_key,
        trim(txn_type) as clean_txn_type,
        coalesce(merchant, 'DIRECT') as resolved_merchant
    from transactions
    where merchant is not null
)
select
    *,
    first_value(merchant) ignore nulls over (partition by txn_type order by txn_time, id rows between unbounded preceding and current row) as first_known_merchant,
    avg(amount) over (partition by txn_type, merchant) as txn_type_avg_amount,
    row_number() over (order by txn_time desc, id) as row_seq,
    sum(amount) over (order by amount, id rows between unbounded preceding and current row) as cumulative_amount,
    sum(amount) over (partition by account_id, txn_type order by txn_time, id rows between unbounded preceding and current row) as running_amount
from valid
