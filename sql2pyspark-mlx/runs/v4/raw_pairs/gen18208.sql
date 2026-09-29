select
    id,
    status,
    opened_date,
    customer_id,
    first_value(customer_id) over (partition by status order by opened_date desc, id) as first_customer_id
from accounts
where opened_date <= '2024-10-01'
