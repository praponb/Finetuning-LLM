with filtered as (
    select *, cast(quantity as double) as quantity_str
    from inventory
    where store_id in (1, 3)
)
select id, product_id
from filtered
except all
select id, product_id
from inventory
where product_id in (5, 8, 9);
