select id, customer_id, upper(device) as device_upper
from sessions
where device = 'tablet' and country in ('UK', 'US', 'SG')
order by customer_id, id desc
