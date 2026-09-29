select monthly_fee
from subscriptions
where status <> 'PAUSED'
intersect
select amount
from orders
where amount between 1060 and 1470;
