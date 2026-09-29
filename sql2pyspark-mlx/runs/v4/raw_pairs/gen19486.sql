select date_format(last_restock, 'yyyy-MM') as last_restock_month, sum(quantity) as total_quantity
from inventory
where last_restock > '2025-04-30' or store_id = 6
group by 1
order by total_quantity
