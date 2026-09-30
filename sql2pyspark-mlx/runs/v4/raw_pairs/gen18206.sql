select
    id,
    city,
    credit_limit,
    country,
    initcap(trim(segment)) segment_title
from customers
order by id desc
limit 5
