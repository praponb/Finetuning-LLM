select
    month(shipped_date) as shipped_month,
    date_trunc('MONTH', delivered_date) as delivered_month,
    max(shipped_date) as last_shipped_date,
    sum(case when carrier = 'UPS' then 1 else 0 end) as ups_count
from shipments
where order_id = 2 or delivered_date between '2024-05-01' and '2024-11-02'
group by month(shipped_date), date_trunc('MONTH', delivered_date);
