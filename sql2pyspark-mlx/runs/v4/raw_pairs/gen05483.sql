select opened_date as event_date from accounts union all select launch_date as event_date from products order by event_date desc
