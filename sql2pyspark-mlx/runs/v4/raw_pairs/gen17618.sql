with with_windows as (
    -- Ranking and gap features
    select
        *,
        row_number() over (partition by session_id, event_type order by event_time, id) as event_seq,
        avg(duration_sec) over (order by cast(event_time as bigint) range between 2592000 preceding and current row) as duration_sec_avg_30d,
        first_value(page) ignore nulls over (order by event_time, id rows between unbounded preceding and current row) as first_known_page,
        row_number() over (partition by page order by event_time desc, id) as seq_no,
        lead(duration_sec, 1) over (order by event_time, id) as next_duration_sec
    from web_events
),
with_dims as (
    -- Attach sessions attributes
    select
        s.*,
        p.browser,
        p.country,
        p.device
    from with_windows s
    inner join sessions p
        on s.session_id = p.id
),
enriched as (
    -- Attach sessions attributes
    select
        s.*,
        d.country as session_country,
        d.browser as session_browser,
        d.device as session_device
    from with_dims s
    inner join sessions d
        on s.session_id = d.id
),
with_dims_web_events as (
    -- Attach sessions attributes
    select
        e.*,
        d.device as session_device_2,
        d.browser as session_browser_2,
        d.country as session_country_2
    from enriched e
    left join sessions d
        on e.session_id = d.id
),
ranked as (
    -- Keep one row per key (latest first)
    select *
    from with_dims_web_events
    qualify row_number() over (partition by session_browser order by seq_no desc, id) = 1
),
metrics as (
    -- Running and moving aggregates
    select
        *,
        lead(next_duration_sec, 1) over (partition by session_country order by duration_sec, id) as next_next_duration_sec,
        avg(next_duration_sec) over w_hist as next_duration_sec_avg_5,
        sum(event_seq) over (order by cast(event_time as bigint) range between 604800 preceding and current row) as event_seq_sum_7d,
        lead(next_duration_sec, 1) over w_hist_2 as next_next_duration_sec_2
    from ranked
    window w_hist as (order by next_duration_sec, id rows between 5 preceding and current row), w_hist_2 as (partition by session_browser, event_type order by seq_no, id)
),
joined as (
    -- Attach sessions attributes
    select
        s.*,
        d.device as session_device_3,
        d.country as session_country_3,
        d.browser as session_browser_3
    from metrics s
    left join sessions d
        on s.session_id = d.id
)
select
    id,
    event_type,
    event_time,
    duration_sec_avg_30d,
    seq_no,
    next_duration_sec,
    browser,
    device,
    session_country
from joined
order by device desc
