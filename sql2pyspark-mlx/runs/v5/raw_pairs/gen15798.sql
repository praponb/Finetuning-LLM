with valid_inventory as (
    select *
    from inventory
    where quantity > 113 and last_restock <= '2023-03-01' and last_restock >= '2023-10-01'
),
summary as (
    select
        store_id,
        product_id,
        count(distinct quantity) as unique_products,
        max(quantity) - min(quantity) as quantity_range
    from valid_inventory
    group by store_id, product_id
)
select
    store_id,
    product_id,
    unique_products,
    quantity_range
from summary
order by unique_products
