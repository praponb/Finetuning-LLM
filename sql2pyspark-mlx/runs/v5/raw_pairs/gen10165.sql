with with_last as (
    -- Latest matching order per row (LATERAL ... LIMIT 1)
    select s.*, top1.discount
    from stores s
    left join lateral (
            select c.discount
            from orders c
            where c.store_id = s.id
            order by c.order_date desc, c.id desc
            limit 1
        ) top1 on true
),
with_last_stores as (
    -- Latest matching order per row (LATERAL ... LIMIT 1)
    select
        s.*,
        latest.order_shipping_fee,
        latest.order_amount
    from with_last s
    join lateral (
            select c.shipping_fee as order_shipping_fee, c.amount as order_amount
            from orders c
            where c.store_id = s.id
            order by c.discount desc, c.id desc
            limit 1
        ) latest on true
),
enriched as (
    -- Window metrics over several different partitions
    select
        *,
        sum(discount) over w_running as running_sum_discount,
        avg(discount) over w_running as running_avg_discount,
        max(discount) over w_running as running_max_discount,
        count(discount) over w_running as running_count_discount,
        lead(order_amount, 1) over w_running_2 as next_order_amount
    from with_last_stores
    where discount < 0.24
    window w_running as (partition by city order by opened_date, id rows between unbounded preceding and current row), w_running_2 as (partition by region order by opened_date, id)
),
kept_keys as (
    -- Set difference on keys
    select city, id
    from enriched
    except
    select city, id
    from stores
    where opened_date >= '2024-01-15'
),
reconciled as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from enriched s
    join kept_keys k on s.city = k.city and s.id = k.id
),
with_windows as (
    -- Running and moving aggregates
    select
        id,
        store_name,
        region,
        order_shipping_fee,
        running_sum_discount,
        cume_dist() over w_recent as running_avg_discount_cume_dist,
        stddev_samp(discount) over (partition by city, region) as city_stddev_samp_discount
    from reconciled
    window w_recent as (order by running_avg_discount desc nulls last)
)
-- Aggregate with conditional metrics
select
    region,
    min(running_sum_discount) as min_running_sum_discount,
    avg(city_stddev_samp_discount) as avg_city_stddev_samp_discount,
    count_if(region = 'North') as north_events
from with_windows
group by region
