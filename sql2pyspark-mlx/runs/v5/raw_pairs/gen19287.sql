select category, sum(stock_qty) as total_stock_qty, round(sum(stock_qty) * 100.0 / sum(sum(stock_qty)) over (), 2) as pct_of_total
from products
where stock_qty < 340
group by category
order by pct_of_total, category
limit 20
