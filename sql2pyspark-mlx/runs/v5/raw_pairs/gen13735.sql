select
    product_id,
    store_id,
    quantity_range,
    stddev_quantity
from (
    select
        product_id,
        store_id,
        max(quantity) - min(quantity) as quantity_range,
        round(stddev(quantity), 2) as stddev_quantity
    from inventory
    where quantity >= 89
    group by product_id, store_id
) as sub
where quantity_range < 33
order by 4, product_id, store_id
limit 5;
