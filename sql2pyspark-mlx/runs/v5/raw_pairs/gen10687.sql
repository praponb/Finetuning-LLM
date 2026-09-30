with flagged as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when lag(started_at) over (partition by device order by started_at, id) is null
                 or started_at > lag(started_at) over (partition by device order by started_at, id) + interval '30' day then 1 else 0 end as session_start_flag
    from sessions
),
sessions_sessions as (
    -- Running sum of the flag = session number
    select *, sum(session_start_flag) over (partition by device order by started_at, id rows between unbounded preceding and current row) as session_no
    from flagged
)
select
    s.id,
    s.customer_id,
    s.device,
    s.browser,
    s.started_at,
    s.session_start_flag,
    case when device rlike '^(tablet|desktop)$' then 'EXTENDED' else 'CORE' end as device_zone,
    (select min(x.balance) from accounts x where x.customer_id = s.customer_id) as min_account_balance
from sessions_sessions s
order by browser desc;
