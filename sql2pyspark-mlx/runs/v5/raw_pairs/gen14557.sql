select
    id,
    status,
    order_id,
    concat(method, '/', status) as method_status,
    case when method in ('card', 'bank_transfer') then 'Group A' else 'Other' end as method_group
from payments
where order_id = 32;
