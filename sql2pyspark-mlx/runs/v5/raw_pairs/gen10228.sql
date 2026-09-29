select
    brand,
    category,
    round(avg(stock_qty), 2) as avg_stock_qty,
    max(launch_date) as last_launch_date,
    max(launch_date) as latest_launch_date
from products
group by brand, category
having round(avg(stock_qty), 2) > 38
order by last_launch_date, brand, category
limit 5
