select id, shipped_date, date_trunc('YEAR', delivered_date) as delivered_date_period
from shipments
where status = 'DELIVERED'
