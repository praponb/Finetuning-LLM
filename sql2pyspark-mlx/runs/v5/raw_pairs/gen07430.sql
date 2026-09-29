select *
from (
    select
        txn_type,
        merchant,
        amount
    from transactions
) src
pivot (
    avg(amount)
    for merchant in ('Starbucks', 'Shell')
);
