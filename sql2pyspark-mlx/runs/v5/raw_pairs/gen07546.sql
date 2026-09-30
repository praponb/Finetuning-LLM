select
    a.opened_date,
    a.account_type,
    a.customer_id,
    c.name,
    c.is_active
from accounts a
left join customers c on a.customer_id = c.id;
