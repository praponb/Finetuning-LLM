select
    date_format(last_restock, 'yyyy-MM') as last_restock_month,
    count(quantity) as quantity_count,
    round(stddev(quantity), 2) as stddev_quantity,
    sum(quantity) as total_quantity
from inventory
where store_id in (1, 3, 4, 6) and (product_id in (1, 3, 5, 7) or quantity >= 89)
group by date_format(last_restock, 'yyyy-MM')
having sum(quantity) >= 1220
order by last_restock_month;
