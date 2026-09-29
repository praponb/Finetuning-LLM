select
    date_format(last_restock, 'yyyy-MM') as last_restock_month,
    store_id,
    count(*) as cnt,
    count(quantity) as quantity_count
from inventory
where quantity >= 84
group by date_format(last_restock, 'yyyy-MM'), store_id;
