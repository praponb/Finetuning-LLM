select
    case when amount >= 1230 then 'HIGH' else 'LOW' end as amount_tier,
    status,
    count_if(method = 'paypal') as paypal_events,
    avg(amount) filter (where method = 'promptpay') as promptpay_avg_amount,
    stddev_samp(amount) as amount_stddev,
    grouping(case when amount >= 1230 then 'HIGH' else 'LOW' end) as grouping_id
from payments
group by cube (case when amount >= 1230 then 'HIGH' else 'LOW' end, status)
having promptpay_avg_amount >= 0
order by amount_stddev
