with valid as (
    select
        id,
        order_id,
        method,
        amount,
        paid_at,
        status,
        case when method = 'cash' then 1 else 0 end as is_cash,
        substring(status, 1, 4) as status_prefix,
        coalesce(method, 'OTHER') as resolved_method
    from payments
    where method is distinct from 'promptpay'
      and order_id is not null
),
metrics as (
    select
        id,
        order_id,
        method,
        paid_at,
        status,
        is_cash,
        resolved_method,
        sum(is_cash) over (partition by order_id, status) as order_id_sum_is_cash,
        ntile(5) over (order by amount desc, id) as amount_bucket,
        avg(amount) over (order by is_cash, id rows between 2 preceding and current row) as amount_avg_2,
        ntile(5) over (partition by method order by amount, id) as amount_bucket_2
    from valid
),
enriched as (
    select
        e.*,
        b.discount,
        b.status as order_status
    from metrics e
    left join orders b
        on e.order_id = b.id
),
with_dims as (
    select
        e.id,
        e.order_id,
        e.method,
        e.paid_at,
        e.is_cash,
        e.resolved_method,
        e.amount_bucket,
        e.amount_avg_2,
        e.amount_bucket_2,
        e.discount,
        e.order_status,
        p.status as order_status_2,
        p.amount
    from enriched e
    left join orders p
        on e.order_id <=> p.id
    where p.shipping_fee is not null
)
select
    order_status,
    order_status_2,
    method,
    stddev_samp(amount) as amount_stddev,
    sum(case when order_status_2 = 'COMPLETED' then is_cash else 0 end) as completed_is_cash,
    sum(case when order_status_2 = 'SHIPPED' then amount else 0 end) as shipped_amount,
    percentile_approx(amount_avg_2, array(0.25, 0.5, 0.75)) as amount_avg_2_quartiles,
    size(collect_set(order_status)) as num_order_status,
    approx_percentile(discount, array(0.25, 0.5, 0.75)) as discount_quartiles
from with_dims
group by order_status, order_status_2, method
order by completed_is_cash desc nulls first;
