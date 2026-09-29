select month(opened_date) as opened_month, min(opened_date) as min_opened_date, sum(sqft) as total_sqft
from stores
where store_name like 'Ma%'
group by month(opened_date)
