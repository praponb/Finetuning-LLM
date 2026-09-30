select id, shipped_date
from shipments
where carrier = 'Kerry'
except
select id, shipped_date
from shipments
where carrier is null;
