with valid as (
    select *, quantity - unit_price as line_total
    from order_items
    where product_id is not null
),
metrics as (
    select
        id,
        order_id,
        quantity,
        unit_price,
        max(quantity) over (partition by order_id) as order_id_max_quantity,
        lead(line_total, 1) over w_recent as next_line_total
    from valid
    window w_recent as (partition by product_id, order_id order by unit_price, id)
),
with_windows as (
    select
        *,
        row_number() over w_hist as seq_no,
        ntile(10) over w_hist_2 as unit_price_bucket,
        last_value(order_id_max_quantity, true) over (partition by order_id order by quantity, id rows between unbounded preceding and current row) as last_known_order_id_max_quantity,
        ntile(5) over w_hist_2 as unit_price_bucket_2
    from metrics
    where order_id_max_quantity is not null
    window w_hist as (partition by order_id order by quantity, id), w_hist_2 as (partition by order_id order by unit_price, id)
),
scored as (
    select
        *,
        avg(unit_price_bucket) over (order by unit_price, id rows between 2 preceding and 1 preceding) as unit_price_bucket_avg_2_prior,
        first_value(order_id_max_quantity, true) over (partition by order_id order by next_line_total, id rows between unbounded preceding and current row) as first_known_order_id_max_quantity
    from with_windows
)
select
    o.channel,
    count(*) as num_rows,
    round(avg(e.order_id_max_quantity), 2) as avg_order_id_max_quantity
from scored e
join orders o on e.order_id = o.id
group by o.channel
