select date_trunc('MONTH', launch_date) as launch_month, brand, sum(stock_qty) as total_stock_qty
from products
where category not in ('Grocery', 'Home') and cost >= 450
group by launch_month, brand
having sum(stock_qty) >= 1600
order by launch_month, brand
limit 5;
