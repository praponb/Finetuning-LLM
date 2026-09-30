with enriched as (
    -- Running and moving aggregates
    select *, last_value(browser, true) over w_recent as last_known_browser
    from sessions
    window w_recent as (partition by device, browser order by started_at asc, id asc rows between unbounded preceding and current row)
),
with_window as (
    -- Non-equi join on a validity window
    select
        src.*,
        sub.id as subscription_id,
        sub.plan,
        sub.monthly_fee
    from enriched src
    left join subscriptions sub
        on src.customer_id = sub.customer_id
       and src.started_at between sub.start_date and sub.end_date
),
not_flagged as (
    -- Anti-join via a correlated subquery
    select s.*
    from with_window s
    where not exists (
            select 1
            from web_events ch
            where ch.session_id = s.id
              and ch.duration_sec != 293
        )
),
not_flagged_sessions as (
    -- Anti-join via a correlated subquery
    select s.*
    from not_flagged s
    where not exists (
            select 1
            from web_events c
            where c.session_id = s.id
              and c.event_time <= s.started_at
        )
),
eligible as (
    -- Anti-join via a correlated subquery
    select s.*
    from not_flagged_sessions s
    where not exists (
            select 1
            from web_events x
            where x.session_id = s.id
              and x.page = '/checkout'
        )
),
enriched_sessions as (
    -- Ranking and gap features
    select
        *,
        count(monthly_fee) over (partition by device order by monthly_fee asc, id asc, subscription_id asc rows between unbounded preceding and current row) as running_count_monthly_fee,
        sum(monthly_fee) over (partition by device order by monthly_fee asc, id asc, subscription_id asc rows between unbounded preceding and current row) as running_sum_monthly_fee,
        max(monthly_fee) over (partition by device order by monthly_fee asc, id asc, subscription_id asc rows between unbounded preceding and current row) as running_max_monthly_fee
    from eligible
    where device not in ('tablet', 'desktop')
)
-- Aggregate with conditional metrics
select
    date_trunc('quarter', started_at) as quarter_start,
    date_trunc('week', started_at) as week_start,
    min(started_at) as started_at,
    count(*) filter (where country = 'TH') as th_rows,
    dense_rank() over (partition by date_trunc('quarter', started_at) order by count(*) filter (where country = 'TH') desc) as th_rows_dense_rank
from enriched_sessions
group by quarter_start, week_start
order by th_rows asc;
