with filtered as (
    -- Keep recent, valid rows
    select
        *,
        quantity * unit_price as line_total,
        cast(quantity as double) as quantity_num
    from order_items
    where product_id is null
),
with_windows as (
    -- Running and moving aggregates
    select
        *,
        ntile(3) over (partition by product_id, order_id order by line_total, id) as line_total_bucket,
        first_value(quantity_num) ignore nulls over (partition by order_id order by unit_price, id rows between unbounded preceding and current row) as first_known_quantity_num
    from filtered
),
valid as (
    -- Keep recent, valid rows
    select *, line_total_bucket * quantity_num as line_total_bucket_times_quantity_num
    from with_windows
    where product_id is not null
      and first_known_quantity_num between 34 and 51
),
scored as (
    -- Ranking and gap features
    select
        id,
        order_id,
        product_id,
        unit_price,
        line_total,
        first_known_quantity_num,
        lag(quantity, 2) over (partition by order_id order by line_total_bucket, id) as prev_quantity,
        first_value(quantity_num, true) over w_grp as first_known_quantity_num_2,
        sum(unit_price) over w_grp as cumulative_unit_price
    from valid
    window w_grp as (partition by order_id order by quantity, id rows between unbounded preceding and current row)
),
metrics as (
    -- Ranking and gap features
    select
        id,
        order_id,
        prev_quantity,
        avg(prev_quantity) over (partition by product_id, order_id order by first_known_quantity_num, id rows between unbounded preceding and current row) as running_avg_prev_quantity,
        sum(prev_quantity) over (partition by product_id, order_id order by first_known_quantity_num, id rows between unbounded preceding and current row) as running_sum_prev_quantity,
        min(prev_quantity) over (partition by product_id, order_id order by first_known_quantity_num, id rows between unbounded preceding and current row) as running_min_prev_quantity,
        max(prev_quantity) over (partition by product_id, order_id order by first_known_quantity_num, id rows between unbounded preceding and current row) as running_max_prev_quantity,
        sum(cumulative_unit_price) over (partition by product_id) as product_id_sum_cumulative_unit_price
    from scored
),
grouped as (
    -- Grouped metrics
    select
        order_id,
        case when running_max_prev_quantity >= 9.0 then 'PREMIUM' else 'STANDARD' end as running_max_prev_quantity_tier,
        approx_percentile(running_min_prev_quantity, array(0.25, 0.5, 0.75)) as running_min_prev_quantity_quartiles,
        count(*) as row_count,
        grouping(order_id) as grouping_id
    from metrics
    group by cube (order_id, case when running_max_prev_quantity >= 9.0 then 'PREMIUM' else 'STANDARD' end)
    having row_count < 1000
)
select
    s.order_id,
    s.running_max_prev_quantity_tier,
    s.running_min_prev_quantity_quartiles,
    s.grouping_id,
    (select min(x.unit_price) from order_items x where x.order_id = s.order_id) as min_order_item_unit_price
from grouped s
order by grouping_id;
