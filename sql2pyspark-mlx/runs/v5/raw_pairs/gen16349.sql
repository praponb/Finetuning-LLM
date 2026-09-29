with base as (
    select
        id,
        order_id,
        method,
        amount,
        paid_at,
        status,
        length(method) as method_length
    from payments
    where method in ('bank_transfer', 'card')
),
with_dims as (
    select
        s.id,
        s.order_id,
        s.method,
        s.amount,
        s.status,
        s.method_length,
        o.amount as order_amount,
        o.status as order_status,
        o.discount
    from base s
    left join orders o
        on s.order_id = o.id
),
with_windows as (
    select
        id,
        status,
        method_length,
        min(method_length) over (partition by order_status order by amount, id rows between unbounded preceding and current row) as running_min_method_length,
        sum(method_length) over (partition by order_status order by amount, id rows between unbounded preceding and current row) as running_sum_method_length,
        max(method_length) over (partition by order_status order by amount, id rows between unbounded preceding and current row) as running_max_method_length,
        ntile(5) over (partition by order_id order by method_length, id) as method_length_bucket
    from with_dims
),
filtered as (
    select
        id,
        method_length,
        running_min_method_length,
        running_sum_method_length,
        running_max_method_length,
        method_length_bucket,
        case when running_max_method_length >= 87 then 'High' when running_max_method_length >= 20 then 'Medium' else 'Low' end as running_max_method_length_level
    from with_windows
    where running_min_method_length > 57
)
select
    case when method_length >= 70 then 'PREMIUM' else 'STANDARD' end as method_length_tier,
    case when running_sum_method_length >= 400 then 'PREMIUM' else 'STANDARD' end as running_sum_method_length_tier,
    count(distinct id) as distinct_ids,
    count(distinct id) as distinct_ids_2
from filtered
group by case when method_length >= 70 then 'PREMIUM' else 'STANDARD' end, case when running_sum_method_length >= 400 then 'PREMIUM' else 'STANDARD' end
order by distinct_ids desc nulls first
