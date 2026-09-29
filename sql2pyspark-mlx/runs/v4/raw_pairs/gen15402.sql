with flagged as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when lag(txn_time) over (partition by txn_type order by txn_time, id) is null
                 or txn_time > lag(txn_time) over (partition by txn_type order by txn_time, id) + interval 60 days then 1 else 0 end as is_new_session
    from transactions
),
sessions_transactions as (
    -- Running sum of the flag = session number
    select *, sum(is_new_session) over (partition by txn_type order by txn_time, id rows between unbounded preceding and current row) as burst_id
    from flagged
)
-- Grouped metrics
select
    case when is_new_session >= 1 then 'PREMIUM' else 'STANDARD' end as is_new_session_tier,
    count(*) as cnt,
    size(collect_set(merchant)) as num_merchants,
    size(collect_set(merchant)) as num_merchants_2,
    sort_array(collect_set(txn_type)) as txn_type_list
from sessions_transactions
group by case when is_new_session >= 1 then 'PREMIUM' else 'STANDARD' end
order by num_merchants_2 desc nulls first
