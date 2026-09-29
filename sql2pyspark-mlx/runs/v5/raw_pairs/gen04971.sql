select
    i.id,
    i.quantity,
    ss.product_id,
    case when ss.revenue >= 3900 then 'High' when ss.revenue >= 1400 then 'Medium' else 'Low' end as revenue_tier
from inventory i
left outer join store_sales ss on i.store_id = ss.store_id and i.product_id = ss.product_id
order by ss.product_id desc
