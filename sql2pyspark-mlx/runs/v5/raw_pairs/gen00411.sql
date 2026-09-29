select date_format(p.launch_date, 'yyyy-MM') as launch_month, count(i.quantity) as quantity_count, max(i.quantity) - min(i.quantity) as quantity_range
from inventory i
full outer join products p on i.product_id = p.id
group by date_format(p.launch_date, 'yyyy-MM')
having count(i.quantity) > 1;
