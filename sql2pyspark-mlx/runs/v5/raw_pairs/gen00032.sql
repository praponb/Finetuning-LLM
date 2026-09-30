with deduped as (
    select id, order_id
    from (
        select *, row_number() over (partition by order_id order by unit_price desc nulls first, id) as dedup_rank
        from order_items
    ) ranked
    where dedup_rank = 1
),
enriched as (
    select
        s.id,
        s.order_id,
        p.order_date,
        p.amount,
        p.status
    from deduped s
    join orders p
        on s.order_id = p.id
    where p.customer_id in (16, 30, 33, 39)
),
deduped_order_items as (
    select
        id,
        order_id,
        amount
    from (
        select *, row_number() over (partition by status order by amount desc nulls last, id) as rank_in_group
        from enriched
    ) x
    where rank_in_group = 1
),
with_windows as (
    select
        *,
        min(amount) over w_running as last4_min_amount,
        sum(amount) over w_running as last4_sum_amount,
        max(amount) over w_running as last4_max_amount,
        avg(amount) over w_running as last4_avg_amount,
        ntile(3) over (order by amount, id) as amount_bucket
    from deduped_order_items
    window w_running as (partition by order_id order by amount, id rows between 3 preceding and current row)
)
select
    case when last4_max_amount >= 1780 then 'LARGE' else 'SMALL' end as last4_max_amount_tier,
    case when amount_bucket >= 8 then 'PREMIUM' else 'STANDARD' end as amount_bucket_tier,
    percentile_approx(last4_min_amount, 0.95) as last4_min_amount_p95,
    max(last4_max_amount) as max_last4_max_amount,
    count(distinct order_id) as distinct_order_ids,
    count(distinct order_id) as distinct_order_ids_2,
    count(distinct order_id) as distinct_order_ids_3
from with_windows
group by 1, 2
order by last4_max_amount_tier nulls last;
