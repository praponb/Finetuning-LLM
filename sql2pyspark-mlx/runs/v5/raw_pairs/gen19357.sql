select *
from (
    select *, row_number() over (partition by product_id order by quantity nulls first, id) as row_num
    from inventory
) ranked
where row_num = 1
