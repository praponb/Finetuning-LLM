with cleaned as (
    -- Filter the raw rows and derive helper columns
    select
        *,
        concat_ws('-', event_type, page) event_type_page_key,
        to_date(event_time) event_time_date,
        coalesce(page, 'OTHER') page_filled
    from web_events
    where page is not null
),
with_dims as (
    -- Attach sessions attributes
    select
        e.*,
        d.device,
        d.browser,
        d.country
    from cleaned e
    left join sessions d
        on e.session_id = d.id
),
with_windows as (
    -- Window metrics over several different partitions
    select
        id,
        session_id,
        event_type,
        page,
        event_time,
        duration_sec,
        event_type_page_key,
        event_time_date,
        page_filled,
        device,
        browser,
        country,
        avg(duration_sec) over (partition by device order by event_time_date, id rows between 3 preceding and 1 preceding) duration_sec_avg_3_prior,
        row_number() over (order by event_time, id) row_seq
    from with_dims
),
kept_keys as (
    -- Set difference on keys
    select page
    from with_windows
    except
    select page
    from web_events
    where duration_sec = 406
),
reconciled as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from with_windows s
    inner join kept_keys k on s.page = k.page
)
-- Multi-level aggregation
select
    case when duration_sec_avg_3_prior >= 452 then 'LARGE' else 'SMALL' end as duration_sec_avg_3_prior_tier,
    case when duration_sec >= 395 then 'PREMIUM' else 'STANDARD' end as duration_sec_tier,
    date_trunc('quarter', event_time) as quarter_start,
    min(event_time) as first_event_time,
    count_if(event_type = 'search') as search_events,
    size(collect_set(country)) as num_countries
from reconciled
group by rollup (case when duration_sec_avg_3_prior >= 452 then 'LARGE' else 'SMALL' end, case when duration_sec >= 395 then 'PREMIUM' else 'STANDARD' end, date_trunc('quarter', event_time))
having size(collect_set(country)) < 1;
