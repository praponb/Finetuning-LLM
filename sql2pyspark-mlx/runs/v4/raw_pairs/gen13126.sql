select case when quantity >= 254 then 'high' when quantity >= 118 then 'mid' else 'low' end as quantity_bucket, sum(quantity) as total_quantity
from inventory
group by case when quantity >= 254 then 'high' when quantity >= 118 then 'mid' else 'low' end
order by quantity_bucket;
