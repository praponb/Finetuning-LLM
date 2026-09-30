select revenue
from store_sales
where store_id in (1, 5)
except
select amount
from transactions
where account_id in (19, 36)
