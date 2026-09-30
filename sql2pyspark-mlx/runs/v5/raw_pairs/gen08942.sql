select id, shipped_date, carrier, order_id, last_day(shipped_date) as shipped_date_month_end, ceil(cost) as cost_whole from shipments where shipped_date > '2025-09-01';
