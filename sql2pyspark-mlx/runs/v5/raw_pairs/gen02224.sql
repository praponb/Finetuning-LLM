select
    i.id,
    i.store_id,
    ss.units,
    ss.revenue + ss.units as revenue_per_unit
from inventory i
join store_sales ss on i.store_id = ss.store_id and i.product_id = ss.product_id
where ss.store_id in (1, 2, 5, 6)
order by store_id, i.id desc
