select id, method, length(method) as method_length
from payments
where order_id in (21, 22, 29, 32)
