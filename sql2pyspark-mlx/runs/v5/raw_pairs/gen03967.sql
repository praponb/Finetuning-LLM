select opened_date, 'account' as origin
from accounts
union all
select last_restock, 'inventory' as origin
from inventory
where quantity = 206
