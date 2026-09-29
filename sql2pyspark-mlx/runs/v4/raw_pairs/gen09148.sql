select shipped_date as event_date from shipments union all select order_date as event_date from orders order by event_date;
