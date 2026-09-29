select
    id,
    status,
    shipped_date,
    delivered_date,
    last_value(delivered_date) over (partition by status order by shipped_date, id rows between unbounded preceding and unbounded following) as last_delivered_date
from shipments;
