select shipped_date as event_date from shipments union all select sale_date as event_date from store_sales where revenue <= 4000
