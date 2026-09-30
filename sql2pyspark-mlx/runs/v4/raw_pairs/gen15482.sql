with valid as (
    -- Basic cleansing and defaults
    select
        *,
        round(quantity * 1.1, 2) as quantity_scaled,
        round(quantity, 2) as quantity_rounded
    from inventory
    where quantity >= 159
),
metrics as (
    -- Window metrics over several different partitions
    select
        id,
        store_id,
        product_id,
        quantity,
        last_restock,
        quantity_scaled,
        quantity_rounded,
        lag(quantity_rounded, 1) over (partition by store_id order by quantity, id) as prev_quantity_rounded,
        lead(quantity, 1, 0) over (partition by store_id order by quantity_rounded, id) as next_quantity,
        avg(quantity_scaled) over (partition by product_id order by last_restock, id rows between 5 preceding and current row) as quantity_scaled_avg_5
    from valid
),
latest as (
    -- QUALIFY-style window filter
    select *
    from (
        select *, rank() over (partition by store_id order by next_quantity desc) as rn
        from metrics
    ) ranked
    where rn = 1
),
metrics_inventory as (
    -- Ranking and gap features
    select
        id,
        store_id,
        product_id,
        quantity,
        last_restock,
        quantity_scaled,
        quantity_rounded,
        prev_quantity_rounded,
        next_quantity,
        quantity_scaled_avg_5,
        rn,
        last_value(quantity_scaled_avg_5) ignore nulls over (partition by product_id order by next_quantity, id rows between unbounded preceding and current row) as last_known_quantity_scaled_avg_5,
        sum(quantity_scaled_avg_5) over (partition by product_id order by rn, id rows between unbounded preceding and current row) as running_quantity_scaled_avg_5,
        row_number() over (order by last_restock, id) as seq_no,
        ntile(10) over (partition by product_id order by prev_quantity_rounded desc, id) as prev_quantity_rounded_bucket
    from latest
),
base as (
    -- Basic cleansing and defaults
    select
        *,
        seq_no / nullif(quantity_rounded, 0) as seq_no_per_quantity_rounded,
        add_months(last_restock, 1) as last_restock_plus_months
    from metrics_inventory
    where last_restock >= date '2023-09-16'
)
select
    s.id,
    s.product_id,
    s.last_restock,
    s.quantity_scaled,
    s.quantity_rounded,
    s.prev_quantity_rounded,
    s.quantity_scaled_avg_5,
    s.rn,
    s.seq_no,
    (select max(x.units) from store_sales x where x.product_id = s.product_id) as max_store_sale_units
from base s
order by id desc
