with flagged as (
    select *, case when event_time - lag(event_time) over (partition by session_id order by event_time, id) > interval 7 days then 1 else 0 end as is_new_session
    from web_events
),
islands as (
    select *, sum(is_new_session) over (partition by session_id order by event_time, id rows between unbounded preceding and current row) as island_id
    from flagged
)
select
    island_id,
    sort_array(collect_set(event_type)) as event_type_list,
    count(case when page = '/checkout' then 1 end) as checkout_count,
    sum(case when page = '/home' then is_new_session else 0 end) as home_is_new_session,
    round(avg(is_new_session), 2) as avg_is_new_session,
    count(*) filter (where event_type = 'purchase') as purchase_rows,
    round(avg(island_id), 2) as avg_island_id
from islands
group by island_id
having count(*) filter (where event_type = 'purchase') > 1
order by avg_island_id desc nulls first
