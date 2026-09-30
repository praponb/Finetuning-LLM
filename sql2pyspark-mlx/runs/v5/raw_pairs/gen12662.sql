select id, cost
from shipments
qualify row_number() over (partition by status order by delivered_date desc nulls first, id) = 1;
