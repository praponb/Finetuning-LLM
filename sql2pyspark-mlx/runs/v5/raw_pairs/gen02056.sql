with metrics as (
    -- Ranking and gap features
    select
        id,
        device,
        started_at,
        country,
        first_value(country, true) over w_recent as first_known_country,
        first_value(browser) ignore nulls over (partition by device order by started_at, id rows between unbounded preceding and current row) as first_known_browser,
        first_value(browser, true) over w_ordered as first_known_browser_2,
        last_value(country, true) over (order by started_at, id rows between unbounded preceding and current row) as last_known_country,
        row_number() over (partition by country order by started_at, id) as seq_no
    from sessions
    where country = 'JP'
    window w_recent as (partition by country order by started_at, id rows between unbounded preceding and current row), w_ordered as (partition by device, country order by started_at, id rows between unbounded preceding and current row)
),
flagged as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when lag(started_at) over (partition by country order by started_at, id) is null
                 or started_at > lag(started_at) over (partition by country order by started_at, id) + interval 30 days then 1 else 0 end as session_start_flag
    from metrics
),
sessionized as (
    -- Running sum of the flag = session number
    select
        id,
        device,
        started_at,
        country,
        first_known_browser_2,
        last_known_country,
        seq_no,
        sum(session_start_flag) over (partition by country order by started_at, id rows between unbounded preceding and current row) as burst_id
    from flagged
),
metrics_sessions as (
    -- Running and moving aggregates
    select
        *,
        max(seq_no) over w_grp as last3_max_seq_no,
        avg(seq_no) over w_grp as last3_avg_seq_no,
        min(seq_no) over w_grp as last3_min_seq_no
    from sessionized
    window w_grp as (partition by device order by burst_id, id rows between 2 preceding and current row)
),
latest as (
    -- Deduplicate with ROW_NUMBER
    select
        id,
        device,
        country,
        first_known_browser_2,
        last_known_country,
        last3_max_seq_no,
        last3_avg_seq_no,
        last3_min_seq_no
    from metrics_sessions
    qualify row_number() over (partition by country order by last3_min_seq_no desc, id) = 1
),
latest_sessions as (
    -- Deduplicate with ROW_NUMBER
    select *, row_number() over (partition by country order by last3_max_seq_no nulls last, id) as latest_rank
    from latest
    qualify latest_rank = 1
)
-- Grouped metrics
select
    device,
    country,
    count(distinct country) as distinct_countries,
    count_if(device = 'tablet') as tablet_events,
    sort_array(collect_set(device)) as device_list,
    grouping(device) as grouping_id
from latest_sessions
group by cube (device, country)
having count_if(device = 'tablet') > 1
order by device desc;
