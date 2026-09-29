select paid_day, order_id, promptpay_count
from (
    select to_date(paid_at) as paid_day, order_id, sum(case when method = 'promptpay' then 1 else 0 end) as promptpay_count
    from payments
    where amount >= 1100
    group by to_date(paid_at), order_id
) as x
where promptpay_count < 65
order by 1 desc
