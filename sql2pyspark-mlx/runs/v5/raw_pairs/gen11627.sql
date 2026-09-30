select id, stock_qty, cost
from products
where product_name is not null
order by cost desc
