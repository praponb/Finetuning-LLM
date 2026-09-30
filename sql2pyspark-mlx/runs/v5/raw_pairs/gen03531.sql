select status, sort_array(collect_set(status)) as status_list
from orders
group by status;
