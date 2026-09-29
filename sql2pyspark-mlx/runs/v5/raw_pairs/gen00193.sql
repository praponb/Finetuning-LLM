with filtered as (
    select
        *,
        date_format(event_time, 'yyyy-MM') as event_time_label,
        coalesce(page, 'N/A') as page_clean
    from web_events
    where event_time > timestamp '2023-09-09 09:00:00'
      and page != '/checkout'
),
filtered_web_events as (
    select
        *,
        case when page in ('/cart', '/product') then 'Priority' else 'Other' end as page_group,
        datediff(current_date(), event_time) as days_since_event_time,
        coalesce(page, 'OTHER') as page_clean_2
    from filtered
    where event_time > date '2023-09-04'
      and event_type = 'page_view'
),
matched_keys as (
    select id, page
    from filtered_web_events
    except all
    select id, page
    from web_events
    where event_type != 'add_to_cart'
),
reconciled as (
    select s.*
    from filtered_web_events s
    inner join matched_keys k on s.id = k.id and s.page = k.page
),
with_dims as (
    select
        a.id,
        a.session_id,
        a.event_type,
        a.duration_sec,
        a.page_group,
        a.days_since_event_time,
        a.page_clean_2,
        d.device
    from reconciled a
    left join sessions d
        on a.session_id = d.id
)
select
    case when duration_sec >= 401 then 'HIGH' else 'LOW' end as duration_sec_tier,
    count(*) filter (where event_type = 'purchase') as purchase_rows,
    count(*) filter (where device = 'mobile') as mobile_rows,
    count(*) filter (where device = 'mobile') as mobile_rows_2
from with_dims
group by case when duration_sec >= 401 then 'HIGH' else 'LOW' end
having mobile_rows_2 > 1
order by purchase_rows desc nulls first;
