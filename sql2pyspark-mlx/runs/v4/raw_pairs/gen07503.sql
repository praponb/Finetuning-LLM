select *
from (
    select *, row_number() over (partition by order_id order by quantity desc nulls first, id) as row_num
    from order_items
) t
where row_num = 1
