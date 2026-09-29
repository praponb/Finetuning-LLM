with cleaned as (
    select
        *,
        upper(merchant) as merchant_upper,
        quarter(txn_time) as txn_time_quarter
    from transactions
    where account_id in (18, 34)
)
select
    *,
    lag(amount, 2) over (partition by txn_type order by txn_time_quarter asc, id asc) as prev_amount,
    lag(txn_time_quarter, 2) over (partition by account_id, merchant order by amount asc, id asc) as prev_txn_time_quarter,
    avg(txn_time_quarter) over (partition by account_id order by cast(txn_time as bigint) asc range between 7776000 preceding and current row) as txn_time_quarter_avg_90d
from cleaned;
