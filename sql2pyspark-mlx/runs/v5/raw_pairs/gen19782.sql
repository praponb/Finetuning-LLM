select last_restock, store_id, case when quantity >= 230 then 'High' when quantity >= 67 then 'Medium' else 'Low' end as quantity_tier
from inventory
where product_id in (5, 10);
