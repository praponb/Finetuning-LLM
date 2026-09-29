with valid as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        session_id,
        event_type,
        page,
        duration_sec,
        concat_ws('/', page, event_type) as page_event_type_key,
        coalesce(page, 'NONE') as page_clean
    from web_events
    where event_time >= date '2023-04-02'
      and page not in ('/cart', '/home')
),
deduped as (
    -- Deduplicate with ROW_NUMBER
    select
        id,
        session_id,
        event_type,
        page,
        duration_sec,
        page_event_type_key,
        page_clean
    from valid
    qualify rank() over (partition by page, event_type order by duration_sec desc) = 1
),
scored as (
    -- Ranking and gap features
    select
        id,
        session_id,
        page,
        duration_sec,
        page_event_type_key,
        lag(duration_sec, 1, 0) over (partition by event_type, page order by duration_sec, id) as prev_duration_sec,
        rank() over (partition by page order by duration_sec desc nulls first) as duration_sec_rank,
        row_number() over (partition by session_id order by duration_sec, id) as seq_no
    from deduped
),
ranked as (
    -- QUALIFY-style window filter
    select
        id,
        session_id,
        page,
        page_event_type_key,
        duration_sec_rank,
        seq_no
    from (
        select *, row_number() over (partition by page, session_id order by duration_sec_rank desc nulls last, id) as dedup_rank
        from scored
    ) ranked
    where dedup_rank = 1
),
scored_web_events as (
    -- Ranking and gap features
    select
        *,
        min(seq_no) over (partition by session_id, page order by duration_sec_rank, id rows between unbounded preceding and current row) as running_min_seq_no,
        max(seq_no) over (partition by session_id, page order by duration_sec_rank, id rows between unbounded preceding and current row) as running_max_seq_no,
        avg(seq_no) over (partition by session_id, page order by duration_sec_rank, id rows between unbounded preceding and current row) as running_avg_seq_no,
        last_value(duration_sec_rank) ignore nulls over (partition by page order by seq_no, id rows between unbounded preceding and current row) as last_known_duration_sec_rank
    from ranked
    where seq_no between 6 and 8
),
valid_web_events as (
    -- Keep recent, valid rows
    select
        id,
        session_id,
        page,
        page_event_type_key,
        duration_sec_rank,
        seq_no,
        running_min_seq_no,
        running_max_seq_no,
        running_avg_seq_no,
        last_known_duration_sec_rank,
        ceil(duration_sec_rank) as duration_sec_rank_whole
    from scored_web_events
    where page in ('/product', '/search')
),
cleaned as (
    -- Keep recent, valid rows
    select
        *,
        greatest(duration_sec_rank, running_max_seq_no) as greatest_duration_sec_rank_running_max_seq_no,
        coalesce(page, 'NONE') as page_clean
    from valid_web_events
    where page is not null
),
grouped as (
    -- Aggregate with conditional metrics
    select
        case when duration_sec_rank_whole >= 84 then 'LARGE' else 'SMALL' end as duration_sec_rank_whole_tier,
        session_id,
        seq_no,
        count(*) filter (where page = '/product') as product_rows,
        sum(case when page = '/product' then duration_sec_rank else 0 end) as product_duration_sec_rank,
        count(*) as cnt,
        sort_array(collect_set(page)) as page_list,
        count(distinct session_id) as distinct_session_ids,
        percent_rank() over (partition by case when duration_sec_rank_whole >= 84 then 'LARGE' else 'SMALL' end order by count(*) desc) as cnt_percent_rank
    from cleaned
    group by 1, 2, 3
)
select
    duration_sec_rank_whole_tier,
    session_id,
    seq_no,
    cnt,
    page_list,
    cnt_percent_rank
from grouped;
