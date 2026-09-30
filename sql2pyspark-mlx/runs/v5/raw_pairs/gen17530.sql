select
    id,
    opened_date,
    status,
    trim(status) as clean_status
from accounts
where month(opened_date) in (1, 5, 9) or customer_id in (9, 13, 34, 40)
order by opened_date, status desc, id
limit 10
