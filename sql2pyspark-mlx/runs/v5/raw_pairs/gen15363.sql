select
    *,
    min(unit_price) over (partition by product_id order by unit_price, id rows between unbounded preceding and current row) as running_min_unit_price,
    avg(unit_price) over (partition by product_id order by unit_price, id rows between unbounded preceding and current row) as running_avg_unit_price,
    sum(unit_price) over (partition by product_id order by unit_price, id rows between unbounded preceding and current row) as running_sum_unit_price,
    count(unit_price) over (partition by product_id order by unit_price, id rows between unbounded preceding and current row) as running_count_unit_price,
    cume_dist() over (partition by product_id order by unit_price nulls last) as unit_price_cume_dist
from order_items
where order_id = 12
