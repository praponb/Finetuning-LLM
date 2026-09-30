with with_prev as (
    -- Previous event time per key
    select *, lag(started_at) over (partition by customer_id order by started_at asc, id asc) as prev_started_at
    from sessions
),
flagged as (
    -- Flag rows that start a new session
    select *, case when prev_started_at is null or started_at > prev_started_at + interval 30 days then 1 else 0 end as new_group_flag
    from with_prev
),
sessions_sessions as (
    -- Running sum of the flag = session number
    select
        id,
        customer_id,
        browser,
        started_at,
        country,
        sum(new_group_flag) over (partition by customer_id order by started_at asc, id asc rows between unbounded preceding and current row) as burst_id
    from flagged
),
with_windows as (
    -- Running and moving aggregates
    select
        id,
        customer_id,
        browser,
        cume_dist() over w_part as burst_id_cume_dist,
        row_number() over (partition by customer_id order by started_at desc, id asc) as event_seq
    from sessions_sessions
    where browser in ('Firefox', 'Chrome', 'Edge')
    window w_part as (partition by browser order by burst_id asc)
),
with_windows_sessions as (
    -- Running and moving aggregates
    select
        *,
        last_value(browser) ignore nulls over w_grp as last_known_browser,
        avg(event_seq) over (partition by customer_id, browser order by burst_id_cume_dist asc, id asc rows between 5 preceding and 1 preceding) as event_seq_avg_5_prior
    from with_windows
    window w_grp as (partition by browser, customer_id order by event_seq asc, id asc rows between unbounded preceding and current row)
),
matched_keys as (
    -- Set intersection on keys
    select id
    from with_windows_sessions
    intersect all
    select id
    from sessions
    where browser in ('Chrome', 'Safari')
),
reconciled as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from with_windows_sessions s
    inner join matched_keys k on s.id = k.id
),
scored as (
    -- Running and moving aggregates
    select
        *,
        stddev_samp(event_seq) over (partition by browser) as browser_stddev_samp_event_seq,
        cume_dist() over w_ordered as burst_id_cume_dist_cume_dist
    from reconciled
    window w_ordered as (partition by customer_id order by burst_id_cume_dist asc)
),
joined as (
    -- Attach customers attributes
    select
        s.id,
        s.customer_id,
        s.last_known_browser,
        s.event_seq_avg_5_prior,
        s.browser_stddev_samp_event_seq,
        s.burst_id_cume_dist_cume_dist,
        c.name
    from scored s
    inner join customers c
        on s.customer_id = c.id
)
select
    id,
    last_known_browser,
    browser_stddev_samp_event_seq,
    burst_id_cume_dist_cume_dist
from joined;
