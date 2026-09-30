select is_flagged, merchant, concat_ws(', ', sort_array(collect_set(merchant))) as merchant_values
from transactions
group by is_flagged, merchant
order by 3
