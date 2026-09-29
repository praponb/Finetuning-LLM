select
    date_trunc('quarter', started_at) as quarter_start,
    device,
    sort_array(collect_set(device)) as device_list,
    count(case when device = 'tablet' then 1 end) as tablet_count,
    sort_array(collect_set(browser)) as browser_list,
    size(collect_set(browser)) as num_browsers
from sessions
group by cube (date_trunc('quarter', started_at), device)
having num_browsers > 1
order by tablet_count desc nulls last
