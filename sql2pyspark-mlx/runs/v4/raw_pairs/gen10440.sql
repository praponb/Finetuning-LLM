select
    id,
    order_id,
    status,
    last_day(delivered_date) as delivered_date_month_end
from shipments;
