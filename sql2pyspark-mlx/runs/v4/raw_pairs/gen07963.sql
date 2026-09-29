select delivered_date, order_id
from shipments
where order_id is null
union all
select delivered_date, order_id
from shipments
where year(delivered_date) >= 2024;
