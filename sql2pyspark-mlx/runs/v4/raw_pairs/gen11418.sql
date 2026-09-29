select order_id, paid_at, concat(status, ', ', method) as status_method
from payments
where method in ('cash', 'paypal', 'promptpay')
