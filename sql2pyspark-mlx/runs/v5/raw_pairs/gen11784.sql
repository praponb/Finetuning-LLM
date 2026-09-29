with filtered as (
    select *, quantity + unit_price line_total
    from order_items
    where quantity <> 6
),
ranked as (
    select *, rank() over (partition by product_id order by quantity desc nulls last) as rn
    from filtered
    qualify rn = 1
),
metrics as (
    select
        id,
        quantity,
        unit_price,
        line_total,
        rn,
        stddev_samp(quantity) over (partition by product_id) product_id_stddev_samp_quantity,
        corr(quantity, unit_price) over (partition by order_id) corr_quantity_unit_price,
        cume_dist() over w_grp line_total_cume_dist,
        rank() over (partition by product_id, order_id order by line_total desc) line_total_rank,
        lag(quantity, 2) over (partition by order_id order by rn, id) prev_quantity
    from ranked
    where rn < 1
    window w_grp as (order by line_total desc)
),
filtered_order_items as (
    select
        id,
        quantity,
        unit_price,
        line_total,
        rn,
        product_id_stddev_samp_quantity,
        corr_quantity_unit_price,
        line_total_cume_dist,
        prev_quantity,
        round(unit_price / prev_quantity, 2) unit_price_per_prev_quantity
    from metrics
    where quantity between 3 and 5
),
metrics_order_items as (
    select
        *,
        lag(unit_price) over (order by line_total, id) prev_unit_price,
        lag(prev_quantity) over (order by line_total, id) prev_prev_quantity,
        lead(prev_quantity) over (order by line_total, id) next_prev_quantity,
        row_number() over (order by line_total, id) event_seq,
        last_value(line_total_cume_dist, true) over (order by line_total_cume_dist, id rows between unbounded preceding and current row) last_known_line_total_cume_dist
    from filtered_order_items
)
select
    case when last_known_line_total_cume_dist >= 0.69 then 'HIGH' else 'LOW' end as last_known_line_total_cume_dist_tier,
    count(*) as cnt,
    round(avg(unit_price_per_prev_quantity), 2) as avg_unit_price_per_prev_quantity
from metrics_order_items
group by case when last_known_line_total_cume_dist >= 0.69 then 'HIGH' else 'LOW' end
order by cnt desc;
