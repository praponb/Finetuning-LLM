select id, product_id, case when quantity >= 183 then 'High' when quantity >= 34 then 'Medium' else 'Low' end as quantity_tier
from inventory
order by id desc
