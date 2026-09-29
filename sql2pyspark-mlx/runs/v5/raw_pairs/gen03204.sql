with base as (
    select
        id,
        session_id,
        event_type,
        page,
        event_time,
        duration_sec,
        ceil(duration_sec) as duration_sec_whole,
        coalesce(page, 'DIRECT') as resolved_page
    from web_events
    where event_time >= to_timestamp('2023-05-25 04:00:00')
      and page is not null
),
latest as (
    select *
    from (
        select *, row_number() over (partition by page, event_type order by duration_sec desc nulls first, id) as dedup_rank
        from base
    ) t
    where dedup_rank = 1
),
gaps as (
    select *, case when event_time - lag(event_time) over (partition by page order by event_time, id) > interval '90' day then 1 else 0 end as new_group_flag
    from latest
),
islands as (
    select *, sum(new_group_flag) over (partition by page order by event_time, id rows between unbounded preceding and current row) as burst_id
    from gaps
),
scored as (
    select
        id,
        session_id,
        event_type,
        page,
        resolved_page,
        dedup_rank,
        new_group_flag,
        burst_id,
        avg(duration_sec) over (partition by session_id order by burst_id, id rows between 6 preceding and 1 preceding) as duration_sec_avg_6_prior,
        min(burst_id) over (partition by event_type) as event_type_min_burst_id,
        lag(duration_sec, 1) over (partition by page order by duration_sec, id) as prev_duration_sec,
        row_number() over (partition by event_type, session_id order by new_group_flag desc, id) as seq_no
    from islands
),
with_dims as (
    select
        s.*,
        b.country,
        b.browser
    from scored s
    join sessions b
        on s.session_id = b.id
)
select
    event_type,
    case when burst_id >= 8 then 'LARGE' else 'SMALL' end as burst_id_tier,
    approx_percentile(seq_no, array(0.25, 0.5, 0.75)) as seq_no_quartiles,
    size(collect_set(country)) as num_countries,
    count_if(country = 'DE') as de_events,
    approx_percentile(prev_duration_sec, 0.95) as prev_duration_sec_p95,
    max(seq_no) as max_seq_no,
    grouping_id() as grouping_level
from with_dims
group by cube (event_type, case when burst_id >= 8 then 'LARGE' else 'SMALL' end)
order by de_events nulls first;
