with valid as (
    -- Basic cleansing and defaults
    select *, quantity - unit_price as line_total
    from order_items
    where product_id is not null
),
metrics as (
    -- Ranking and gap features
    select
        *,
        ntile(3) over (partition by order_id order by line_total, id) as line_total_bucket,
        lag(line_total, 2) over (partition by order_id order by unit_price, id) as prev_line_total
    from valid
),
joined as (
    -- Attach orders attributes
    select src.*, b.shipping_fee
    from metrics src
    left join orders b
        on src.order_id = b.id
),
filtered as (
    -- Keep recent, valid rows
    select
        id,
        order_id,
        quantity,
        unit_price,
        line_total,
        prev_line_total,
        shipping_fee,
        greatest(shipping_fee, quantity) as greatest_shipping_fee_quantity
    from joined
    where order_id in (7, 25)
),
metrics_order_items as (
    -- Window metrics over several different partitions
    select
        *,
        sum(prev_line_total) over w_ordered as running_prev_line_total,
        avg(greatest_shipping_fee_quantity) over (partition by order_id order by prev_line_total, id rows between 2 preceding and current row) as greatest_shipping_fee_quantity_avg_2,
        avg(quantity) over w_recent as quantity_avg_5,
        first_value(unit_price, true) over (partition by order_id order by line_total, id rows between unbounded preceding and current row) as first_known_unit_price
    from filtered
    window w_ordered as (partition by order_id order by prev_line_total, id rows between unbounded preceding and current row), w_recent as (order by shipping_fee, id rows between 5 preceding and current row)
),
with_dims as (
    -- Attach orders attributes
    select
        s.*,
        o.channel,
        o.discount
    from metrics_order_items s
    left join orders o
        on s.order_id = o.id
),
joined_order_items as (
    -- Attach orders attributes
    select
        s.*,
        p.order_date,
        p.shipping_fee as order_shipping_fee,
        p.status
    from with_dims s
    left join orders p
        on s.order_id = p.id
)
-- Grouped metrics
select
    channel,
    date_trunc('quarter', order_date) as quarter_start,
    status,
    count(case when status = 'PENDING' then 1 end) as pending_count,
    count(*) filter (where channel = 'partner') as partner_rows
from joined_order_items
group by channel, date_trunc('quarter', order_date), status
