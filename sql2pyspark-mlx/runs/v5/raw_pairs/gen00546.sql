with src as (
    select *
    from inventory
    where store_id in (1, 3, 6) and store_id in (2, 5)
),
product_id_summary as (
    select product_id, max(last_restock) as last_last_restock
    from src
    group by product_id
)
select product_id, last_last_restock
from product_id_summary;
