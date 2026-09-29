with filtered_transactions as (
    select *
    from transactions
    where merchant in ('Amazon', 'Shell') and account_id = 12
),
merchant_summary as (
    select merchant, sum(case when txn_type = 'DEBIT' then 1 else 0 end) as debit_count
    from filtered_transactions
    group by merchant
)
select *
from merchant_summary
order by merchant;
