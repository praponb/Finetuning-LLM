select
    case when amount >= 1220 then 'high' when amount >= 380 then 'mid' else 'low' end as amount_bucket,
    status,
    sort_array(collect_set(method)) as method_list,
    max(amount) as max_amount,
    concat_ws(', ', sort_array(collect_set(status))) as status_values
from payments
where method in ('cash', 'bank_transfer') and paid_at >= '2024-07-01' and order_id in (3, 6, 16)
group by amount_bucket, status
order by amount_bucket desc
