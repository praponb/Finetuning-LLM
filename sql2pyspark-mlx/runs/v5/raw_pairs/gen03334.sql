with valid as (
    -- Basic cleansing and defaults
    select
        id,
        session_id,
        event_type,
        duration_sec,
        cast(duration_sec as double) as duration_sec_num
    from web_events
    where page is not null
      and page in ('/search', '/cart', '/checkout')
),
with_windows as (
    -- Running and moving aggregates
    select
        id,
        session_id,
        count(duration_sec) over (partition by session_id, event_type order by duration_sec_num, id rows between 2 preceding and current row) as last3_count_duration_sec,
        avg(duration_sec) over (partition by session_id, event_type order by duration_sec_num, id rows between 2 preceding and current row) as last3_avg_duration_sec,
        sum(duration_sec) over (partition by session_id, event_type order by duration_sec_num, id rows between 2 preceding and current row) as last3_sum_duration_sec,
        stddev_samp(duration_sec_num) over (partition by session_id) as session_id_stddev_samp_duration_sec_num
    from valid
),
enriched as (
    -- Attach sessions attributes
    select
        src.*,
        b.device,
        b.country
    from with_windows src
    left join sessions b
        on src.session_id = b.id
    where hour(b.started_at) between 7 and 12
),
ranked as (
    -- Deduplicate with ROW_NUMBER
    select *
    from (
        select *, row_number() over (partition by session_id order by last3_sum_duration_sec nulls first, id) as latest_rank
        from enriched
    ) t
    where latest_rank = 1
),
matched_keys as (
    -- Set intersection on keys
    select id
    from ranked
    intersect all
    select id
    from web_events
    where event_time >= '2023-11-01'
),
reconciled as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from ranked s
    inner join matched_keys k on s.id = k.id
)
-- Grouped metrics
select
    case when session_id_stddev_samp_duration_sec_num >= 61 then 'PREMIUM' else 'STANDARD' end as session_id_stddev_samp_duration_sec_num_tier,
    case when last3_count_duration_sec >= 18 then 'PREMIUM' else 'STANDARD' end as last3_count_duration_sec_tier,
    size(collect_set(device)) as num_devices,
    round(avg(last3_sum_duration_sec), 2) as avg_last3_sum_duration_sec,
    count(*) as row_count
from reconciled
group by 1, 2
order by num_devices desc
