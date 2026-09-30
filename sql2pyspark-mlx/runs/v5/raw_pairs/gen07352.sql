with latest_match as (
    select s.*, top1.page
    from sessions s
    left join lateral (
            select c.page
            from web_events c
            where c.session_id = s.id
            order by c.duration_sec desc, c.id desc
            limit 1
        ) top1 on true
),
scored as (
    select
        id,
        customer_id,
        device,
        started_at,
        country,
        page,
        first_value(country) ignore nulls over (partition by device order by started_at, id rows between unbounded preceding and current row) as first_known_country,
        row_number() over (partition by country order by started_at, id) as row_seq,
        last_value(page) ignore nulls over (partition by page order by started_at, id rows between unbounded preceding and current row) as last_known_page,
        row_number() over (partition by device order by started_at, id) as event_seq
    from latest_match
),
latest_match_sessions as (
    select s.*, latest.web_event_duration_sec
    from scored s
    left join lateral (
            select c.duration_sec as web_event_duration_sec
            from web_events c
            where c.session_id = s.id
            order by c.duration_sec desc, c.id desc
            limit 1
        ) latest on true
),
scored_sessions as (
    select
        id,
        device,
        country,
        page,
        first_known_country,
        row_seq,
        last_known_page,
        event_seq,
        web_event_duration_sec,
        lead(event_seq) over w_ordered as next_event_seq,
        lag(event_seq) over w_ordered as prev_event_seq,
        lag(web_event_duration_sec) over w_ordered as prev_web_event_duration_sec,
        lead(web_event_duration_sec) over w_ordered as next_web_event_duration_sec,
        row_number() over w_ordered as row_seq_2,
        sum(event_seq) over w_part as running_event_seq
    from latest_match_sessions
    where row_seq = 4
    window w_ordered as (partition by page order by web_event_duration_sec, id), w_part as (partition by country order by web_event_duration_sec, id rows between unbounded preceding and current row)
),
deduped as (
    select *, row_number() over (partition by country order by next_web_event_duration_sec desc, id) as rn
    from scored_sessions
    qualify rn = 1
),
scored_sessions_2 as (
    select
        id,
        device,
        country,
        row_seq,
        last_known_page,
        web_event_duration_sec,
        prev_event_seq,
        prev_web_event_duration_sec,
        row_seq_2,
        rn,
        ntile(4) over (partition by page, country order by row_seq, id) as row_seq_bucket,
        lead(row_seq, 1) over (partition by page order by prev_event_seq, id) as next_row_seq,
        round(row_seq / sum(row_seq) over (partition by page, device), 4) as row_seq_share
    from deduped
)
select
    id,
    device,
    country,
    row_seq,
    web_event_duration_sec,
    prev_event_seq,
    prev_web_event_duration_sec,
    rn,
    next_row_seq
from scored_sessions_2
order by web_event_duration_sec desc;
