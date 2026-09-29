with base as (
    select
        id,
        account_id,
        txn_type,
        txn_time,
        merchant,
        is_flagged,
        to_date(txn_time) as txn_time_date,
        round(amount * 0.9, 2) as amount_adjusted,
        coalesce(merchant, 'N/A') as merchant_filled
    from transactions
    where merchant is not null
)
select txn_type, account_id
from base
except
select txn_type, account_id
from transactions
where merchant not in ('Starbucks', 'Grab', '7-Eleven')
