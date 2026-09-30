select
    id,
    customer_id,
    status,
    opened_date,
    row_number() over (partition by customer_id, status order by opened_date desc, id) row_num
from accounts
order by opened_date;
