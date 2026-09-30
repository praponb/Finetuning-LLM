with numbered as (
    select
        id,
        product_id,
        quantity,
        store_id,
        row_number() over (partition by product_id order by quantity desc, id) rnk
    from inventory
    where product_id in (6, 7, 9)
)
select
    id,
    product_id,
    quantity,
    store_id
from numbered
where rnk <= 5
order by quantity;
