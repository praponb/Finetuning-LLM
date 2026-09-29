with gaps as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when txn_time - lag(txn_time) over (partition by merchant order by txn_time asc, id asc) > interval 60 days then 1 else 0 end as session_start_flag
    from transactions
),
sessionized as (
    -- Running sum of the flag = session number
    select *, sum(session_start_flag) over (partition by merchant order by txn_time asc, id asc rows between unbounded preceding and current row) as session_no
    from gaps
)
-- Multi-level aggregation
select
    case when amount >= 3700 then 'HIGH' else 'LOW' end as amount_tier,
    txn_type,
    stddev_samp(amount) as amount_stddev,
    (unix_timestamp(max(txn_time)) - unix_timestamp(min(txn_time))) / 60 as span_minutes
from sessionized
group by amount_tier, txn_type
having span_minutes >= 0;
