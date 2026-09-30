select
    id,
    carrier,
    cost,
    delivered_date
from shipments
where status = 'LOST' and status = 'DELIVERED'
