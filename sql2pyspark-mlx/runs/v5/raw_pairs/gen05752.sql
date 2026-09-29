select
    date_trunc('MONTH', last_restock) as last_restock_month,
    store_id,
    max(last_restock) as last_last_restock,
    max(last_restock) as latest_last_restock,
    count(distinct quantity) as unique_products
from inventory
where last_restock > '2023-12-21'
group by last_restock_month, store_id
order by last_restock_month, store_id
limit 10
