select id, product_id, quantity
from (
    select *, row_number() over (partition by product_id order by quantity desc, id) as row_num
    from order_items
) as ranked
where row_num <= 5
