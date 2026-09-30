select *
from inventory
qualify rank() over (partition by product_id order by quantity nulls first) = 1;
