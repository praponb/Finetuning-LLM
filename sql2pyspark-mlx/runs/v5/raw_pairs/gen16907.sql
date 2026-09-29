with flagged as (
    select *, case when txn_time - lag(txn_time) over (partition by txn_type order by txn_time, id) > interval '7' day then 1 else 0 end as is_new_burst
    from transactions
),
islands as (
    select *, sum(is_new_burst) over (partition by txn_type order by txn_time, id rows between unbounded preceding and current row) as island_id
    from flagged
)
select
    account_id,
    count_if(txn_type = 'DEBIT') as debit_events,
    count(distinct account_id) as distinct_account_ids,
    count_if(merchant = 'Shell') as shell_events
from islands
group by account_id
order by distinct_account_ids nulls first
