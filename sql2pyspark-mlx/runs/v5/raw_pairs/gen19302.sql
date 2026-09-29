with deduped as (
    select
        id,
        event_type,
        page
    from (
        select *, row_number() over (partition by event_type order by duration_sec desc nulls first, id asc) as latest_rank
        from web_events
    ) ranked
    where latest_rank = 1
)
select
    page,
    sort_array(collect_set(event_type)) as event_type_list,
    count(*) as num_rows,
    count(*) filter (where page = '/home') as home_rows,
    count_if(page = '/cart') as cart_events,
    count(*) as row_count,
    sort_array(collect_set(page)) as page_list
from deduped
group by rollup (page);
