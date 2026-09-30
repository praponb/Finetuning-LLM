select sale_date
from store_sales
union all
select start_date
from subscriptions
where plan = 'basic'
