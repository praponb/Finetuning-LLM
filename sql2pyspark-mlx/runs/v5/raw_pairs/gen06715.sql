select
    quantity,
    unit_price,
    order_id,
    case when unit_price >= 440 then 'High' when unit_price >= 100 then 'Medium' else 'Low' end unit_price_level,
    unit_price - quantity line_total
from order_items
where not (unit_price is not null and order_id in (12, 20, 33))
order by order_id desc
