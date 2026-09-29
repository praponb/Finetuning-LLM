select date_format(shipped_date, 'yyyy-MM') as shipped_month, min(delivered_date) as min_delivered_date
from shipments
where year(delivered_date) >= 2023
group by shipped_month
