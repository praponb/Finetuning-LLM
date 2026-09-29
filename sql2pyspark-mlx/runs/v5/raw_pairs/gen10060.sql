with filtered as (
    -- Basic cleansing and defaults
    select *, if(is_flagged, 'Yes', 'No') as is_flagged_label
    from transactions
    where txn_time > to_date('2023-04-07')
      and merchant is not null
),
flagged as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when txn_time - lag(txn_time) over (partition by account_id order by txn_time, id) > interval 30 days then 1 else 0 end as is_new_session
    from filtered
),
sessionized as (
    -- Running sum of the flag = session number
    select *, sum(is_new_session) over (partition by account_id order by txn_time, id rows between unbounded preceding and current row) as session_no
    from flagged
)
-- Multi-level aggregation
select
    case when is_new_session >= 1 then 'LARGE' else 'SMALL' end as is_new_session_tier,
    case when session_no >= 8 then 'PREMIUM' else 'STANDARD' end as session_no_tier,
    (unix_timestamp(max(txn_time)) - unix_timestamp(min(txn_time))) / 60 as span_minutes,
    round(avg(amount), 2) as avg_amount
from sessionized
group by case when is_new_session >= 1 then 'LARGE' else 'SMALL' end, case when session_no >= 8 then 'PREMIUM' else 'STANDARD' end
having avg_amount >= 0
order by avg_amount desc;
