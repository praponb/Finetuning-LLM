with cleaned as (
    -- Basic cleansing and defaults
    select
        *,
        concat_ws('-', page, event_type) page_event_type_key,
        round(duration_sec, 1) duration_sec_rounded,
        coalesce(page, 'DIRECT') page_clean
    from web_events
    where session_id in (2, 12)
),
enriched as (
    -- Attach sessions attributes
    select
        e.*,
        d.device,
        d.country,
        d.browser
    from cleaned e
    left join sessions d
        on e.session_id = d.id
),
scored as (
    -- Ranking and gap features
    select
        id,
        session_id,
        event_type,
        page,
        event_time,
        duration_sec,
        page_event_type_key,
        duration_sec_rounded,
        page_clean,
        device,
        country,
        browser,
        avg(duration_sec_rounded) over w_ordered duration_sec_rounded_avg_2,
        ntile(5) over (partition by device order by duration_sec, id) duration_sec_bucket
    from enriched
    window w_ordered as (order by event_time, id rows between 2 preceding and current row)
),
cleaned_web_events as (
    -- Basic cleansing and defaults
    select
        id,
        session_id,
        event_type,
        page,
        event_time,
        duration_sec,
        page_event_type_key,
        duration_sec_rounded,
        device,
        browser,
        duration_sec_rounded_avg_2,
        duration_sec_bucket + duration_sec_rounded_avg_2 duration_sec_bucket_plus_duration_sec_rounded_avg_2
    from scored
    where event_time >= date '2023-10-05'
      and device is not null
)
-- Multi-level aggregation
select
    date_trunc('week', event_time) as week_start,
    case when duration_sec_bucket_plus_duration_sec_rounded_avg_2 >= 66 then 'PREMIUM' else 'STANDARD' end as duration_sec_bucket_plus_duration_sec_rounded_avg_2_tier,
    count(*) filter (where event_type = 'purchase') as purchase_rows,
    sort_array(collect_set(browser)) as browser_list
from cleaned_web_events
group by date_trunc('week', event_time), case when duration_sec_bucket_plus_duration_sec_rounded_avg_2 >= 66 then 'PREMIUM' else 'STANDARD' end
order by duration_sec_bucket_plus_duration_sec_rounded_avg_2_tier
