select shipped_date, order_id, substring(status, 1, 4) as status_prefix
from shipments
where carrier = 'UPS' and order_id in (2, 3, 8, 30)
order by shipped_date, status_prefix desc;
