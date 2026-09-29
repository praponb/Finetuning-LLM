select *
from (
    select
        account_id,
        txn_type,
        merchant,
        amount
    from transactions
) src
pivot (
    avg(amount) as value_sum,
    count(1) as cnt
    for merchant in ('7-Eleven' as v_7_eleven, 'Amazon' as amazon)
);
