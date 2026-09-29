select cost
from shipments
intersect
select monthly_fee
from subscriptions
where monthly_fee <= 220
order by cost;
