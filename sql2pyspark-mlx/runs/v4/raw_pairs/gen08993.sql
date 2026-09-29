with valid as (
    -- Filter the raw rows and derive helper columns
    select
        *,
        date_format(last_restock, 'MMM yyyy') as last_restock_label,
        last_day(last_restock) as last_restock_month_end
    from inventory
    where last_restock > date '2024-03-07'
),
kept_keys as (
    -- Set intersection on keys
    select store_id
    from valid
    intersect
    select store_id
    from inventory
    where quantity between 106 and 156
),
kept_rows as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from valid s
    join kept_keys k on s.store_id = k.store_id
),
filtered as (
    -- Basic cleansing and defaults
    select *, date_trunc('week', last_restock) as last_restock_period
    from kept_rows
    where last_restock_month_end < '2024-04-13'
)
select
    s.id,
    s.store_id,
    s.product_id,
    s.quantity,
    s.last_restock_label,
    s.last_restock_month_end,
    s.last_restock_period,
    (select min(t2.quantity) from inventory t2 where t2.store_id = s.store_id) as min_inventory_quantity
from filtered s
