with valid_transactions as (
    select *, datediff(current_date(), txn_time) as days_since_txn_time
    from transactions
    where merchant in ('Shell', 'Amazon', 'Lazada')
),
transactions_stats as (
    select case when year(txn_time) < 2024 then 'before_2024' when year(txn_time) = 2024 then '2024' else 'after_2024' end as txn_cohort, count(distinct days_since_txn_time) as unique_accounts
    from valid_transactions
    group by case when year(txn_time) < 2024 then 'before_2024' when year(txn_time) = 2024 then '2024' else 'after_2024' end
)
select txn_cohort, unique_accounts
from transactions_stats
where unique_accounts >= 8
order by unique_accounts desc
