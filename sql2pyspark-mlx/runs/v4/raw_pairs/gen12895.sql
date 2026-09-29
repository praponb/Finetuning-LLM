select
    store_id,
    product_id,
    case when quantity >= 203 then 'high' when quantity >= 67 then 'mid' else 'low' end as quantity_bucket,
    max(quantity) - min(quantity) as quantity_range
from inventory
group by store_id, product_id, quantity_bucket
having count(*) < 4
order by 3 desc;
