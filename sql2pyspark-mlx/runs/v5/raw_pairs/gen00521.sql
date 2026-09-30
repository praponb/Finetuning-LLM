with scored as (
    -- Running and moving aggregates
    select
        id,
        order_id,
        product_id,
        quantity,
        unit_price,
        ntile(3) over (order by unit_price, id) as unit_price_bucket,
        max(quantity) over (partition by product_id) as product_id_max_quantity,
        count(unit_price) over (partition by order_id) as order_id_count_unit_price
    from order_items
),
latest as (
    -- QUALIFY-style window filter
    select *, rank() over (partition by product_id order by quantity desc) as row_num
    from scored
    qualify row_num = 1
),
enriched as (
    -- Attach products attributes
    select s.*, d.price
    from latest s
    inner join products d
        on s.product_id = d.id
),
metrics as (
    -- Window metrics over several different partitions
    select
        *,
        percent_rank() over (partition by order_id order by order_id_count_unit_price desc) as order_id_count_unit_price_percent_rank,
        ntile(10) over (partition by order_id order by unit_price, id) as unit_price_bucket_2,
        row_number() over w_grp as row_seq,
        count(price) over w_running as order_id_count_price,
        first_value(product_id_max_quantity) ignore nulls over w_part as first_known_product_id_max_quantity
    from enriched
    window w_grp as (partition by product_id, order_id order by product_id_max_quantity desc, id), w_running as (partition by order_id, product_id), w_part as (order by product_id_max_quantity, id rows between unbounded preceding and current row)
),
deduped as (
    -- Keep one row per key (latest first)
    select *, row_number() over (partition by order_id order by quantity nulls first, id) as row_num_2
    from metrics
    qualify row_num_2 = 1
)
-- Aggregate with conditional metrics
select
    order_id,
    percentile_approx(unit_price_bucket, array(0.25, 0.5, 0.75)) as unit_price_bucket_quartiles,
    min(product_id_max_quantity) as min_product_id_max_quantity,
    round(avg(unit_price_bucket_2), 2) as avg_unit_price_bucket_2,
    count(distinct id) as distinct_ids,
    bit_and(quantity) as quantity_bit_and,
    percentile_approx(product_id_max_quantity, 0.5) as product_id_max_quantity_p50
from deduped
group by order_id
order by quantity_bit_and desc nulls last
