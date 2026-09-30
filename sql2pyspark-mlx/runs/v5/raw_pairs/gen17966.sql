with with_gaps as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when started_at - lag(started_at) over (partition by customer_id order by started_at, id) > interval 7 days then 1 else 0 end as new_group_flag
    from sessions
),
sessionized as (
    -- Running sum of the flag = session number
    select *, sum(new_group_flag) over (partition by customer_id order by started_at, id rows between unbounded preceding and current row) as session_no
    from with_gaps
)
select
    s.id,
    s.customer_id,
    s.browser,
    s.started_at,
    s.country,
    s.new_group_flag,
    s.session_no,
    (select max(a2.balance) from accounts a2 where a2.customer_id = s.customer_id) as max_account_balance
from sessionized s
