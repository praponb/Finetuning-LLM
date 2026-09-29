select
    id,
    customer_id,
    country,
    device,
    case when device = 'mobile' then 1 else 0 end as is_mobile
from sessions
where device = 'mobile' or browser in ('Safari', 'Edge', 'Firefox')
