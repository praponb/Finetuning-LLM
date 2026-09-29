with cleaned as (
    -- Basic cleansing and defaults
    select *, concat_ws('/', page, event_type) as page_event_type_key
    from web_events
    where event_time >= timestamp '2023-05-29 17:00:00'
      and to_date(event_time) < '2025-08-15'
),
latest as (
    -- Deduplicate with ROW_NUMBER
    select *
    from cleaned
    qualify row_number() over (partition by page order by duration_sec desc, id) = 1
),
filtered as (
    -- Basic cleansing and defaults
    select
        id,
        event_type,
        page,
        duration_sec,
        page_event_type_key,
        cast(duration_sec as string) as duration_sec_num
    from latest
    where to_date(event_time) < '2023-12-01'
),
enriched as (
    -- Running and moving aggregates
    select
        id,
        event_type,
        page,
        duration_sec,
        page_event_type_key,
        duration_sec_num,
        lag(duration_sec, 1) over (partition by event_type order by duration_sec, id) as prev_duration_sec,
        row_number() over (order by duration_sec, id) as row_seq
    from filtered
),
filtered_web_events as (
    -- Basic cleansing and defaults
    select
        id,
        page,
        duration_sec,
        page_event_type_key,
        duration_sec_num,
        prev_duration_sec,
        row_seq,
        round(prev_duration_sec / row_seq, 2) as prev_duration_sec_per_row_seq,
        case when page in ('/search', '/product') then 'Priority' else 'Other' end as page_group
    from enriched
    where page is not null
),
enriched_web_events as (
    -- Ranking and gap features
    select
        *,
        dense_rank() over (partition by page order by duration_sec desc nulls first) as duration_sec_dense_rank,
        dense_rank() over (partition by page order by duration_sec desc nulls last) as duration_sec_dense_rank_2
    from filtered_web_events
),
deduped as (
    -- Keep one row per key (latest first)
    select *
    from enriched_web_events
    qualify row_number() over (partition by page order by prev_duration_sec desc nulls last, id) = 1
)
-- Aggregate with conditional metrics
select
    case when row_seq >= 8 then 'HIGH' else 'LOW' end as row_seq_tier,
    size(collect_set(page)) as num_pages,
    sum(case when page = '/product' then duration_sec_dense_rank else 0 end) as product_duration_sec_dense_rank
from deduped
group by case when row_seq >= 8 then 'HIGH' else 'LOW' end;
