select *
from promotions
qualify row_number() over (partition by channel order by discount_pct desc, id) = 1;
