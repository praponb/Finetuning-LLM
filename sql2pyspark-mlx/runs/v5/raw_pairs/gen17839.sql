select last_restock
from inventory
union all
select start_date
from subscriptions;
