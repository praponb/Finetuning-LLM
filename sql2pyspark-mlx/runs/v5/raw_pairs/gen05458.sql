with metrics as (
    -- Ranking and gap features
    select
        *,
        sum(amount) over w_grp as running_amount,
        avg(amount) over (partition by merchant order by txn_time, id rows between 5 preceding and 1 preceding) as amount_avg_5_prior
    from transactions
    where hour(txn_time) between 12 and 20
    window w_grp as (order by amount, id rows between unbounded preceding and current row)
)
select
    s.id,
    s.account_id,
    s.txn_type,
    s.amount,
    s.merchant,
    s.is_flagged,
    s.running_amount,
    s.amount_avg_5_prior,
    (select avg(x.amount) from transactions x where x.account_id = s.account_id) as avg_transaction_amount
from metrics s
order by id desc nulls first, amount desc
