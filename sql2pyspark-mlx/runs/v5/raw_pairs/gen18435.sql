select id, order_id, unit_price
from (
    select *, row_number() over (partition by order_id order by unit_price desc, id) as row_num
    from order_items
) as r
where row_num <= 2;
