select end_date
from subscriptions
where end_date between '2023-06-09' and '2025-02-01'
union all
select last_restock
from inventory
where store_id in (1, 2, 3, 5)
