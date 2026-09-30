select
    year(last_restock) as last_restock_year,
    count(*) as num_inventory,
    count(*) as total_count,
    count(distinct quantity) as unique_stores
from inventory
where store_id in (2, 3, 4, 5) and product_id = 4
group by year(last_restock)
having count(*) >= 5
order by num_inventory desc;
