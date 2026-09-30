with filtered_transactions as (
    select *
    from transactions
    where amount between 1200 and 1600 and account_id in (17, 31, 36, 40) and txn_type = 'DEBIT'
),
transactions_stats as (
    select year(txn_time) as txn_year, round((100 * sum(case when txn_type = 'DEBIT' then 1 else 0 end)) / count(*), 1) as pct_debit, max(amount) - min(amount) as amount_range
    from filtered_transactions
    group by year(txn_time)
)
select *
from transactions_stats;
