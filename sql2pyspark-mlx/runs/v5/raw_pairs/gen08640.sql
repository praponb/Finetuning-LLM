with filtered as (
    select
        id,
        order_id,
        method,
        paid_at,
        length(status) as status_length,
        coalesce(method, 'DIRECT') as method_filled
    from payments
    where method is not null
      and method is distinct from 'card'
),
metrics as (
    select
        id,
        order_id,
        method,
        paid_at,
        status_length,
        count(status_length) over (partition by method, order_id order by paid_at asc, id asc rows between unbounded preceding and current row) as running_count_status_length,
        avg(status_length) over (partition by method, order_id order by paid_at asc, id asc rows between unbounded preceding and current row) as running_avg_status_length,
        sum(status_length) over (partition by method, order_id order by paid_at asc, id asc rows between unbounded preceding and current row) as running_sum_status_length
    from filtered
    where order_id is null
),
cleaned as (
    select
        id,
        order_id,
        method,
        paid_at,
        status_length,
        running_count_status_length,
        running_sum_status_length,
        case when method in ('cash', 'bank_transfer') then 'Tier 1' else 'Other' end as method_group,
        coalesce(method, 'OTHER') as method_clean
    from metrics
    where paid_at > date '2023-09-15'
),
with_dims as (
    select s.*, p.status
    from cleaned s
    left join orders p
        on s.order_id = p.id
),
ranked as (
    select *
    from with_dims
    qualify rank() over (partition by status, order_id order by running_count_status_length desc) = 1
)
select
    order_id,
    status,
    date_trunc('year', paid_at) as year_start,
    min(paid_at) as started_at,
    max(paid_at) as last_paid_at,
    approx_percentile(running_sum_status_length, 0.95) as running_sum_status_length_p95,
    round(avg(status_length), 2) as avg_status_length,
    sort_array(collect_set(status)) as status_list,
    rank() over (partition by order_id order by approx_percentile(running_sum_status_length, 0.95) desc) as running_sum_status_length_p95_rank
from ranked
group by 1, 2, 3
