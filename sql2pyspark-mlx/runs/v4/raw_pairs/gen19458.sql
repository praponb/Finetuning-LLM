select method, paid_at, concat_ws('-', status, method) as status_method_key
from payments
order by paid_at, status_method_key;
