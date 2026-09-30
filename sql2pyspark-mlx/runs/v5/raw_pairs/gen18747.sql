with cleaned as (
    select
        id,
        order_id,
        method,
        amount,
        paid_at,
        status,
        length(method) as method_length,
        coalesce(method, 'NONE') as method_clean
    from payments
    where method is not null
)
select
    case when amount >= 1210 then 'HIGH' else 'LOW' end as amount_tier,
    order_id,
    date_trunc('month', paid_at) as activity_month,
    percentile_approx(amount, 0.95) as amount_p95,
    count(case when method = 'promptpay' then 1 end) as promptpay_count
from cleaned
group by case when amount >= 1210 then 'HIGH' else 'LOW' end, order_id, date_trunc('month', paid_at)
having promptpay_count > 1
order by promptpay_count desc nulls first
