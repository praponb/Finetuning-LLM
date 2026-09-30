with base as (
    select
        *,
        concat_ws('-', page, event_type) as page_event_type_key,
        coalesce(page, 'N/A') as page_filled
    from web_events
    where event_time >= date '2023-09-28'
      and page is not null
)
select
    date_trunc('year', event_time) as year_start,
    page,
    max(duration_sec) filter (where event_type = 'add_to_cart') as add_to_cart_max_duration_sec,
    sort_array(collect_set(page)) as page_list,
    size(collect_set(event_type)) as num_event_types
from base
group by date_trunc('year', event_time), page
