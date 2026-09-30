select id, store_name, add_months(opened_date, 6) as opened_date_plus_months
from stores
where region = 'South' and city in ('Singapore', 'Chiang Mai', 'Bangkok')
