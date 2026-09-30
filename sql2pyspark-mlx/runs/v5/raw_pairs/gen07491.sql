select
    id,
    account_type,
    customer_id,
    opened_date,
    status,
    first_value(status) over (partition by account_type, customer_id order by opened_date, id) as first_status
from accounts
where customer_id = 11
order by first_status, customer_id desc
