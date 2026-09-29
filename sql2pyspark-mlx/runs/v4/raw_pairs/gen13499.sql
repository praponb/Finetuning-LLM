with remaining_keys as (
    select status
    from orders
    intersect all
    select status
    from orders
    where discount > 0.11
),
reconciled as (
    select s.*
    from orders s
    inner join remaining_keys k on s.status = k.status
)
select
    date_trunc('month', order_date) as activity_month,
    date_trunc('year', order_date) as year_start,
    customer_id,
    sum(amount) filter (where status = 'CANCELLED') as cancelled_total_amount,
    percentile_approx(discount, array(0.25, 0.5, 0.75)) as discount_quartiles,
    grouping(date_trunc('month', order_date)) as grouping_id
from reconciled
group by grouping sets ((date_trunc('month', order_date), date_trunc('year', order_date), customer_id), (date_trunc('month', order_date), date_trunc('year', order_date)), ())
