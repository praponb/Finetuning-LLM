with filtered as (
    -- Basic cleansing and defaults
    select
        id,
        customer_id,
        device,
        started_at,
        country,
        upper(browser) as browser_upper
    from sessions
    where started_at >= timestamp '2023-09-28 20:00:00'
      and customer_id = 19
)
-- Aggregate with conditional metrics
select
    date_trunc('month', started_at) as activity_month,
    country,
    date_trunc('week', started_at) as week_start,
    count_if(country = 'UK') as uk_events,
    size(collect_set(country)) as num_countries,
    (unix_timestamp(max(started_at)) - unix_timestamp(min(started_at))) / 60 as duration_min,
    count(*) as row_count,
    grouping(date_trunc('month', started_at)) as grouping_level
from filtered
group by rollup (date_trunc('month', started_at), country, date_trunc('week', started_at))
having count_if(country = 'UK') > 1
order by uk_events desc;
