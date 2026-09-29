with cleaned as (
    -- Keep recent, valid rows
    select
        id,
        txn_type,
        txn_time,
        merchant,
        is_flagged,
        length(txn_type) as txn_type_length
    from transactions
    where txn_time > date '2023-08-25'
      and merchant is not null
),
cleaned_transactions as (
    -- Keep recent, valid rows
    select
        id,
        txn_type,
        is_flagged,
        trim(merchant) as clean_merchant,
        substring(txn_type, 1, 4) as txn_type_prefix,
        coalesce(merchant, 'NONE') as merchant_clean
    from cleaned
    where txn_time > date '2023-05-19'
),
cleaned_transactions_2 as (
    -- Keep recent, valid rows
    select
        id,
        txn_type,
        merchant_clean,
        substring(txn_type, 1, 3) as txn_type_prefix_2
    from cleaned_transactions
    where txn_type <> 'FEE'
)
select
    id,
    txn_type,
    merchant_clean,
    txn_type_prefix_2
from cleaned_transactions_2;
