with valid as (
    -- Basic cleansing and defaults
    select
        id,
        product_id,
        quantity,
        quantity + unit_price as line_total
    from order_items
    where order_id = 34
),
with_dims as (
    -- Attach products attributes
    select src.*, d.product_name
    from valid src
    left join products d
        on src.product_id = d.id
),
latest as (
    -- Keep one row per key (latest first)
    select *, row_number() over (partition by product_id order by quantity nulls first, id) as rn
    from with_dims
    qualify rn = 1
),
cleaned as (
    -- Keep recent, valid rows
    select *, initcap(trim(product_name)) as product_name_title
    from latest
    where product_id is not null
      and rn <= 1
),
metrics as (
    -- Window metrics over several different partitions
    select
        *,
        min(quantity) over w_ordered as product_id_min_quantity,
        row_number() over (order by quantity desc, id) as event_seq,
        last_value(quantity, true) over w_grp as last_known_quantity,
        last_value(line_total, true) over w_grp as last_known_line_total
    from cleaned
    window w_ordered as (partition by product_id), w_grp as (partition by product_id order by quantity, id rows between unbounded preceding and current row)
),
valid_order_items as (
    -- Keep recent, valid rows
    select
        *,
        upper(trim(product_name)) as product_name_normalized,
        substring(product_name, 1, 4) as product_name_prefix
    from metrics
    where quantity < 6
)
-- Aggregate with conditional metrics
select
    product_id,
    count(distinct product_id) as distinct_product_ids,
    round(avg(product_id_min_quantity), 2) as avg_product_id_min_quantity
from valid_order_items
group by product_id
having round(avg(product_id_min_quantity), 2) < 0;
