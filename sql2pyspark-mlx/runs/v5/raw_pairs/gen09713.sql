with joined as (
    -- Attach products attributes
    select src.*, b.product_name
    from order_items src
    left join products b
        on src.product_id = b.id
),
kept_keys as (
    -- Set intersection on keys
    select id, order_id
    from joined
    intersect all
    select id, order_id
    from order_items
    where quantity = 8
),
kept_rows as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from joined s
    inner join kept_keys k on s.id = k.id and s.order_id = k.order_id
),
with_dims as (
    -- Attach orders attributes
    select
        e.*,
        o.status,
        o.shipping_fee,
        o.channel
    from kept_rows e
    left join orders o
        on e.order_id = o.id
),
with_dims_order_items as (
    -- Attach products attributes
    select
        s.*,
        p.launch_date,
        p.product_name as product_product_name
    from with_dims s
    left join products p
        on s.product_id = p.id
),
with_windows as (
    -- Running and moving aggregates
    select
        quantity,
        product_name,
        channel,
        launch_date,
        percent_rank() over w_grp as unit_price_percent_rank,
        sum(shipping_fee) over w_hist as product_id_sum_shipping_fee
    from with_dims_order_items
    window w_grp as (order by unit_price asc nulls first), w_hist as (partition by product_id)
),
grouped as (
    -- Grouped metrics
    select
        case when unit_price_percent_rank >= 0.61 then 'PREMIUM' else 'STANDARD' end as unit_price_percent_rank_tier,
        date_trunc('week', launch_date) as week_start,
        count(*) as cnt,
        round(sum(quantity) / nullif(count(case when channel = 'store' then 1 end), 0), 2) as quantity_per_store,
        bit_or(quantity) as quantity_bit_or,
        percentile_approx(product_id_sum_shipping_fee, 0.5) as product_id_sum_shipping_fee_p50,
        count(case when channel = 'store' then 1 end) as store_count,
        sum(product_id_sum_shipping_fee) filter (where channel = 'mobile') as mobile_total_product_id_sum_shipping_fee
    from with_windows
    group by rollup (case when unit_price_percent_rank >= 0.61 then 'PREMIUM' else 'STANDARD' end, date_trunc('week', launch_date))
    having bit_or(quantity) < 1000
)
select
    unit_price_percent_rank_tier,
    week_start,
    quantity_bit_or,
    product_id_sum_shipping_fee_p50,
    store_count,
    mobile_total_product_id_sum_shipping_fee
from grouped
order by unit_price_percent_rank_tier desc;
