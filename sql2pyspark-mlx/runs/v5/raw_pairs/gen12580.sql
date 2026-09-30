with cleaned as (
    -- Basic cleansing and defaults
    select
        *,
        round(amount * 100, 2) as adjusted_amount,
        amount * shipping_fee as amount_with_shipping,
        coalesce(channel, 'N/A') as channel_clean
    from orders
    where order_date >= to_date('2023-08-31')
      and store_id is not null
),
deduped as (
    -- Deduplicate with ROW_NUMBER
    select *, rank() over (partition by channel order by amount) as latest_rank
    from cleaned
    qualify latest_rank = 1
),
latest as (
    -- Keep one row per key (latest first)
    select *, row_number() over (partition by store_id, status order by amount_with_shipping, id) as rn
    from deduped
    qualify rn = 1
),
ranked as (
    -- Deduplicate with ROW_NUMBER
    select *
    from latest
    qualify rank() over (partition by status, channel order by rn desc) = 1
),
enriched as (
    -- Window metrics over several different partitions
    select
        *,
        sum(discount) over w_running as running_sum_discount,
        avg(discount) over w_running as running_avg_discount,
        count(discount) over w_running as running_count_discount,
        max(discount) over w_running as running_max_discount
    from ranked
    window w_running as (partition by customer_id order by rn, id rows between unbounded preceding and current row)
),
ranked_orders as (
    -- QUALIFY-style window filter
    select
        id,
        customer_id,
        channel_clean,
        latest_rank,
        rn,
        running_avg_discount,
        running_max_discount
    from (
        select *, row_number() over (partition by store_id order by order_date desc, id) as dedup_rank
        from enriched
    ) x
    where dedup_rank = 1
),
eligible as (
    -- Semi-join via a correlated subquery
    select s.*
    from ranked_orders s
    where exists (
            select 1
            from order_items x
            where x.order_id = s.id
              and x.unit_price between 110 and 150
        )
)
-- Native SQL UNPIVOT (wide to long)
select *
from (
    select
        id,
        customer_id,
        running_max_discount,
        running_avg_discount
    from eligible
) src
unpivot (
    metric_value for kpi in (running_max_discount, running_avg_discount)
)
