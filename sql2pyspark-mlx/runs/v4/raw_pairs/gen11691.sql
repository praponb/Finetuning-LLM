with RankedTransactions as (
    select
        id,
        txn_type,
        amount,
        row_number() over (partition by txn_type order by amount desc, id) as pos
    from transactions
)
select id, txn_type, amount
from RankedTransactions
where pos <= 5
order by id desc, amount
