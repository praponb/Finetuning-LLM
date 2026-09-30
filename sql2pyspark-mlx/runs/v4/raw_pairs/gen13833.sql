select id, carrier, delivered_date
from shipments
where status = 'LOST'
order by id asc, carrier asc
limit 10
