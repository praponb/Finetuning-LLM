select date_trunc('MONTH', o.order_date) as order_month, count(*) as total_count, max(s.delivered_date) as last_delivered_date
from shipments s
inner join orders o on s.order_id = o.id
group by date_trunc('MONTH', o.order_date)
order by order_month asc
