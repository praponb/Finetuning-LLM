with filtered as (
    -- Basic cleansing and defaults
    select
        id,
        session_id,
        event_type,
        page,
        event_time,
        duration_sec,
        lower(event_type) as event_type_lower
    from web_events
    where event_time > timestamp '2023-05-25 03:00:00'
      and page is not null
),
metrics as (
    -- Window metrics over several different partitions
    select
        *,
        count(duration_sec) over w_ordered as running_count_duration_sec,
        avg(duration_sec) over w_ordered as running_avg_duration_sec,
        min(duration_sec) over w_ordered as running_min_duration_sec,
        first_value(page, true) over w_part as first_known_page
    from filtered
    window w_ordered as (partition by session_id, event_type order by duration_sec, id rows between unbounded preceding and current row), w_part as (partition by event_type order by duration_sec, id rows between unbounded preceding and current row)
),
enriched as (
    -- Running and moving aggregates
    select
        *,
        sum(duration_sec) over w_part as cumulative_duration_sec,
        cume_dist() over (partition by event_type order by running_avg_duration_sec desc nulls first) as running_avg_duration_sec_cume_dist,
        sum(running_min_duration_sec) over (partition by session_id order by cast(event_time as bigint) range between 2592000 preceding and current row) as running_min_duration_sec_sum_30d
    from metrics
    window w_part as (partition by session_id, page order by running_min_duration_sec, id rows between unbounded preceding and current row)
),
with_dims as (
    -- Attach sessions attributes
    select
        a.id,
        a.session_id,
        a.page,
        a.event_time,
        a.duration_sec,
        a.event_type_lower,
        a.running_avg_duration_sec,
        a.first_known_page,
        a.cumulative_duration_sec,
        a.running_avg_duration_sec_cume_dist,
        a.running_min_duration_sec_sum_30d,
        s.device,
        s.country
    from enriched a
    inner join sessions s
        on a.session_id = s.id
    where hour(s.started_at) between 9 and 17
),
scored as (
    -- Running and moving aggregates
    select
        *,
        cume_dist() over (partition by device order by running_avg_duration_sec_cume_dist desc nulls last) as running_avg_duration_sec_cume_dist_cume_dist,
        row_number() over (partition by page order by running_avg_duration_sec_cume_dist desc, id) as row_seq,
        stddev_samp(running_avg_duration_sec_cume_dist) over (partition by session_id, country) as session_id_stddev_samp_running_avg_duration_sec_cume_dist
    from with_dims
)
-- Join a dimension and aggregate
select
    dim.country as session_country,
    src.country as web_event_country,
    count(*) as cnt,
    max(src.running_min_duration_sec_sum_30d) as max_running_min_duration_sec_sum_30d,
    grouping_id() as level_id
from scored src
inner join sessions dim on src.session_id = dim.id
group by grouping sets ((dim.country, src.country), (dim.country), ())
order by web_event_country nulls first;
