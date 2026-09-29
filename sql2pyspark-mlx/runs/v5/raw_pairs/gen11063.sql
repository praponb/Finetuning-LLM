with flagged as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when txn_time - lag(txn_time) over (partition by account_id order by txn_time, id) > interval 30 days then 1 else 0 end as is_new_session
    from transactions
),
sessionized as (
    -- Running sum of the flag = session number
    select *, sum(is_new_session) over (partition by account_id order by txn_time, id rows between unbounded preceding and current row) as session_id
    from flagged
)
-- Grouped metrics
select
    case when is_new_session >= 1 then 'HIGH' else 'LOW' end as is_new_session_tier,
    approx_percentile(amount, 0.5) as amount_p50,
    (unix_timestamp(max(txn_time)) - unix_timestamp(min(txn_time))) / 60 as span_minutes,
    round(sum(session_id) / nullif(count(case when txn_type = 'CREDIT' then 1 end), 0), 2) as session_id_per_credit
from sessionized
group by cube (case when is_new_session >= 1 then 'HIGH' else 'LOW' end)
order by is_new_session_tier desc;
