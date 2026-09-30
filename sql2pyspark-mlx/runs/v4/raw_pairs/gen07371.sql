with valid as (
    -- Filter the raw rows and derive helper columns
    select *, coalesce(duration_sec, 0) as duration_sec_filled
    from web_events
    where event_time >= date '2023-06-28'
),
matched_keys as (
    -- Set difference on keys
    select session_id
    from valid
    except distinct
    select session_id
    from web_events
    where duration_sec > 141
),
kept_rows as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from valid s
    inner join matched_keys k on s.session_id = k.session_id
),
filtered as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        event_type,
        page,
        event_time,
        duration_sec,
        duration_sec_filled,
        length(page) as page_length
    from kept_rows
    where page is not null
),
metrics as (
    -- Running and moving aggregates
    select
        *,
        cume_dist() over (order by page_length desc nulls last) as page_length_cume_dist,
        sum(duration_sec_filled) over (partition by event_type order by cast(event_time as bigint) range between 2592000 preceding and current row) as duration_sec_filled_sum_30d
    from filtered
),
enriched as (
    -- Window metrics over several different partitions
    select
        id,
        event_type,
        page,
        event_time,
        duration_sec,
        duration_sec_filled,
        page_length,
        page_length_cume_dist,
        duration_sec_filled_sum_30d,
        sum(page_length_cume_dist) over (partition by event_type order by cast(event_time as bigint) range between 604800 preceding and current row) as page_length_cume_dist_sum_7d,
        sum(duration_sec) over (partition by event_type, page order by cast(event_time as bigint) range between 2592000 preceding and current row) as duration_sec_sum_30d
    from metrics
)
-- Grouped metrics
select
    event_type,
    count_if(page = '/search') as search_events,
    round(sum(page_length_cume_dist_sum_7d) / nullif(count(case when event_type = 'add_to_cart' then 1 end), 0), 2) as page_length_cume_dist_sum_7d_per_add_to_cart,
    round(avg(duration_sec_sum_30d), 2) as avg_duration_sec_sum_30d
from enriched
group by event_type
having search_events < 1
order by search_events
