select
    id,
    status,
    carrier,
    delivered_date,
    shipped_date,
    first_value(shipped_date) over (partition by status, carrier order by delivered_date desc, id) as first_shipped_date
from shipments;
