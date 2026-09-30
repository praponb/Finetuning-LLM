select
    i.id,
    i.quantity,
    p.brand,
    s.region,
    case when i.quantity >= 264 then 'High' when i.quantity >= 41 then 'Medium' else 'Low' end as quantity_level
from inventory i
left join products p on p.id = i.product_id
inner join stores s on s.id = i.store_id
