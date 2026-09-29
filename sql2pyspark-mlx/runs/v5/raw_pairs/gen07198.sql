select revenue
from store_sales
intersect
select amount
from transactions
where is_flagged
order by revenue desc;
