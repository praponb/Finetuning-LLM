select supplier_id, sum(stock_qty) as total_stock_qty, round(sum(stock_qty) * 100.0 / sum(sum(stock_qty)) over (), 2) as pct_of_total
from products
group by supplier_id;
