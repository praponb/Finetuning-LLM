select order_id, cost, date_sub(shipped_date, 66) as shipped_date_minus_66d
from shipments
where cost between 28 and 44 and year(delivered_date) >= 2023 and cost <= 80;
