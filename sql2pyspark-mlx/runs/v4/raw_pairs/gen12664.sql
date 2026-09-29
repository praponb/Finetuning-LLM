with valid as (
    -- Keep recent, valid rows
    select
        id,
        session_id,
        event_type,
        event_time,
        duration_sec,
        coalesce(page, 'Unknown') as page_filled,
        substring(event_type, 1, 2) as event_type_prefix
    from web_events
    where duration_sec between 162 and 440
),
metrics as (
    -- Ranking and gap features
    select
        *,
        row_number() over w_ordered as row_seq,
        sum(duration_sec) over w_ordered_2 as cumulative_duration_sec,
        sum(duration_sec) over (partition by session_id order by event_time, id rows between unbounded preceding and current row) as cumulative_duration_sec_2,
        avg(duration_sec) over w_hist as duration_sec_avg_3_prior
    from valid
    window w_ordered as (partition by session_id, event_type order by event_time, id), w_ordered_2 as (partition by session_id order by duration_sec, id rows between unbounded preceding and current row), w_hist as (partition by session_id order by duration_sec, id rows between 3 preceding and 1 preceding)
),
deduped as (
    -- QUALIFY-style window filter
    select *, row_number() over (partition by session_id order by duration_sec desc, id) as rank_in_group
    from metrics
    qualify rank_in_group = 1
)
-- Grouped metrics
select
    date_trunc('year', event_time) as year_start,
    round(sum(duration_sec) / nullif(count(case when event_type = 'click' then 1 end), 0), 2) as duration_sec_per_click,
    approx_percentile(duration_sec, array(0.25, 0.5, 0.75)) as duration_sec_quartiles
from deduped
group by date_trunc('year', event_time)
order by year_start
