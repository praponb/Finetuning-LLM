with valid as (
    -- Basic cleansing and defaults
    select
        id,
        session_id,
        event_type,
        page,
        duration_sec,
        date_trunc('MONTH', event_time) event_time_period
    from web_events
    where event_type not in ('page_view', 'search', 'purchase')
),
ranked as (
    -- Deduplicate with ROW_NUMBER
    select *
    from valid
    qualify row_number() over (partition by page order by event_time_period nulls last, id) = 1
),
metrics as (
    -- Window metrics over several different partitions
    select
        *,
        first_value(duration_sec) ignore nulls over (partition by session_id order by event_time_period, id rows between unbounded preceding and current row) first_known_duration_sec,
        row_number() over (partition by event_type order by event_time_period desc, id) row_seq,
        cume_dist() over (order by duration_sec nulls last) duration_sec_cume_dist,
        max(duration_sec) over (partition by page) page_max_duration_sec,
        sum(duration_sec) over (partition by session_id order by event_time_period, id rows between unbounded preceding and current row) running_duration_sec
    from ranked
),
daily as (
    -- Multi-level aggregation
    select
        case when row_seq >= 8 then 'PREMIUM' else 'STANDARD' end as row_seq_tier,
        date_trunc('quarter', event_time_period) as quarter_start,
        round(sum(first_known_duration_sec) / nullif(count(case when event_type = 'search' then 1 end), 0), 2) as first_known_duration_sec_per_search,
        approx_percentile(page_max_duration_sec, array(0.25, 0.5, 0.75)) as page_max_duration_sec_quartiles,
        sort_array(collect_set(event_type)) as event_type_list
    from metrics
    group by row_seq_tier, quarter_start
)
select
    row_seq_tier,
    quarter_start,
    event_type_list
from daily
order by row_seq_tier desc nulls last
