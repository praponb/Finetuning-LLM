select
    id,
    cost,
    carrier,
    order_id,
    weekofyear(shipped_date) as shipped_date_weekofyear
from shipments
where (year(delivered_date) = 2023 and month(delivered_date) in (6, 10, 12)) or status in ('IN_TRANSIT', 'LOST', 'RETURNED');
