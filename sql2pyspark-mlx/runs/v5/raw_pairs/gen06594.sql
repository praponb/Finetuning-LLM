select 'store' as record_type, opened_date
from stores
where sqft between 5719 and 6341
union all
select 'shipment' as record_type, shipped_date
from shipments
order by record_type
