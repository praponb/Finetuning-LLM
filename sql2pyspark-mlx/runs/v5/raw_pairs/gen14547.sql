select category, supplier_id, avg(stock_qty) as avg_stock_qty
from products
group by category, supplier_id
order by supplier_id desc, category asc
limit 10;
