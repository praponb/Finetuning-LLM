with cleaned as (
    -- Basic cleansing and defaults
    select
        id,
        session_id,
        page,
        duration_sec,
        ceil(duration_sec) as duration_sec_whole,
        case when event_type = 'purchase' then 1 else 0 end as is_purchase
    from web_events
    where page is not null
),
with_dims as (
    -- Attach sessions attributes
    select
        s.id,
        s.session_id,
        s.page,
        s.duration_sec,
        s.duration_sec_whole,
        p.browser,
        p.device
    from cleaned s
    left join sessions p
        on s.session_id = p.id
),
enriched as (
    -- Running and moving aggregates
    select
        id,
        browser,
        device,
        ntile(5) over (order by duration_sec_whole, id) as duration_sec_whole_bucket,
        row_number() over (partition by device order by duration_sec_whole, id) as row_seq,
        rank() over w_ordered as duration_sec_rank
    from with_dims
    window w_ordered as (partition by page order by duration_sec desc nulls first)
),
enriched_web_events as (
    -- Running and moving aggregates
    select
        *,
        count(duration_sec_whole_bucket) over (partition by browser) as browser_count_duration_sec_whole_bucket,
        sum(duration_sec_whole_bucket) over w_grp as cumulative_duration_sec_whole_bucket,
        lead(duration_sec_whole_bucket, 1) over w_part as next_duration_sec_whole_bucket,
        dense_rank() over (partition by browser order by duration_sec_rank nulls last) as duration_sec_rank_dense_rank,
        dense_rank() over (partition by device, browser order by duration_sec_whole_bucket nulls last) as duration_sec_whole_bucket_dense_rank
    from enriched
    window w_grp as (partition by browser order by duration_sec_rank, id rows between unbounded preceding and current row), w_part as (partition by browser order by row_seq, id)
)
select
    id,
    browser,
    device,
    duration_sec_whole_bucket,
    browser_count_duration_sec_whole_bucket,
    cumulative_duration_sec_whole_bucket,
    next_duration_sec_whole_bucket,
    duration_sec_rank_dense_rank
from enriched_web_events
order by duration_sec_whole_bucket desc, browser
