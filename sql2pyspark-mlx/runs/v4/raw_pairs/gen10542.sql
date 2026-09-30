with ranked_transactions as (
    select
        id,
        account_id,
        amount,
        txn_type,
        row_number() over (partition by account_id order by amount, id) as rnk
    from transactions
)
select
    id,
    account_id,
    amount,
    txn_type
from ranked_transactions
where rnk <= 5;
