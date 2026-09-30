select last_restock as event_date, 'inventory' as kind
from inventory
union
select opened_date as event_date, 'store' as kind
from stores
where region in ('East', 'North', 'Central');
