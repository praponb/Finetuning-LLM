select order_id, case when method in ('paypal', 'promptpay') then 'Group A' else 'Other' end as method_group
from payments
where status = 'FAILED'
