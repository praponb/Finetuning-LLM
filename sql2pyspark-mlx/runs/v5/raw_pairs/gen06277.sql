select *
from (
    select *, rank() over (partition by order_id order by quantity desc) rn
    from order_items
    where unit_price > 200
) r
where rn = 1
order by 1 desc
