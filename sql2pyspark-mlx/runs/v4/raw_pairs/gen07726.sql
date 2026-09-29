select
    case when amount >= 1300 then 'LARGE' else 'SMALL' end as amount_tier,
    method,
    min(paid_at) as started_at,
    count(*) as num_rows,
    count(distinct order_id) as distinct_order_ids
from payments
group by grouping sets ((case when amount >= 1300 then 'LARGE' else 'SMALL' end, method), (case when amount >= 1300 then 'LARGE' else 'SMALL' end), ());
