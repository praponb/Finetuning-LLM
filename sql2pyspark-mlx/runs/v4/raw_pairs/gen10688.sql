with enriched as (
    select
        e.*,
        b.country,
        b.browser
    from web_events e
    left join sessions b
        on e.session_id = b.id
),
flagged as (
    select *, case when lag(event_time) over (partition by session_id order by event_time, id) is null
                 or event_time > lag(event_time) over (partition by session_id order by event_time, id) + interval '14' day then 1 else 0 end as is_new_session
    from enriched
),
islands as (
    select *, sum(is_new_session) over (partition by session_id order by event_time, id rows between unbounded preceding and current row) as burst_id
    from flagged
),
ranked as (
    select *
    from (
        select *, row_number() over (partition by event_type order by is_new_session, id) as rn
        from islands
    ) ranked
    where rn = 1
)
select
    case when is_new_session >= 1 then 'HIGH' else 'LOW' end as is_new_session_tier,
    page,
    case when duration_sec >= 432 then 'LARGE' else 'SMALL' end as duration_sec_tier,
    sum(case when event_type = 'click' then is_new_session else 0 end) as click_is_new_session,
    round(avg(duration_sec), 2) as avg_duration_sec,
    sum(case when page = '/product' then burst_id else 0 end) as product_burst_id,
    count_if(country = 'US') as us_events,
    sort_array(collect_set(event_type)) as event_type_list,
    max(burst_id) filter (where country = 'DE') as de_max_burst_id
from ranked
group by case when is_new_session >= 1 then 'HIGH' else 'LOW' end, page, case when duration_sec >= 432 then 'LARGE' else 'SMALL' end
order by duration_sec_tier desc nulls first;
