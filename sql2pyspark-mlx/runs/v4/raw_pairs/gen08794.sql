with ranked as (
    -- Deduplicate with ROW_NUMBER
    select *
    from (
        select *, row_number() over (partition by order_id, status order by paid_at desc nulls first, id) as rn
        from payments
    ) x
    where rn = 1
),
flagged as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when paid_at - lag(paid_at) over (partition by status order by paid_at, id) > interval 7 days then 1 else 0 end as is_new_session
    from ranked
),
sessionized as (
    -- Running sum of the flag = session number
    select *, sum(is_new_session) over (partition by status order by paid_at, id rows between unbounded preceding and current row) as session_no
    from flagged
),
gaps as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when paid_at - lag(paid_at) over (partition by method order by paid_at, id) > interval '7' day then 1 else 0 end as is_new_session_2
    from sessionized
),
sessions_payments as (
    -- Running sum of the flag = session number
    select *, sum(is_new_session_2) over (partition by method order by paid_at, id rows between unbounded preceding and current row) as island_id
    from gaps
)
-- Aggregate with conditional metrics
select
    date_trunc('week', paid_at) as week_start,
    case when is_new_session_2 >= 1 then 'LARGE' else 'SMALL' end as is_new_session_2_tier,
    count(*) as event_count,
    round(avg(is_new_session), 2) as avg_is_new_session,
    round(sum(session_no) / nullif(count(case when method = 'card' then 1 end), 0), 2) as session_no_per_card
from sessions_payments
group by date_trunc('week', paid_at), case when is_new_session_2 >= 1 then 'LARGE' else 'SMALL' end
order by avg_is_new_session desc nulls last;
