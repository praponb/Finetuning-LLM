select
    id,
    delivered_date,
    status,
    first_value(status) over (order by delivered_date, id) as first_status
from shipments
where status <> 'RETURNED'
