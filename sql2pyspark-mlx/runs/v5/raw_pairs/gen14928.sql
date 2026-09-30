with scored as (
    -- Ranking and gap features
    select
        *,
        avg(quantity) over w_part as running_avg_quantity,
        min(quantity) over w_part as running_min_quantity,
        sum(quantity) over w_part as running_sum_quantity,
        max(quantity) over w_part as running_max_quantity,
        row_number() over (partition by product_id, order_id order by unit_price, id) as event_seq
    from order_items
    window w_part as (partition by product_id order by unit_price, id rows between unbounded preceding and current row)
),
remaining_keys as (
    -- Set intersection on keys
    select order_id, product_id
    from scored
    intersect all
    select order_id, product_id
    from order_items
    where unit_price between 150 and 240
),
kept_rows as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from scored s
    inner join remaining_keys k on s.order_id = k.order_id and s.product_id = k.product_id
),
valid as (
    -- Basic cleansing and defaults
    select
        id,
        order_id,
        quantity,
        unit_price,
        running_avg_quantity,
        running_min_quantity,
        running_sum_quantity,
        event_seq,
        case when running_max_quantity >= 8.6 then 'High' when running_max_quantity >= 2.1 then 'Medium' else 'Low' end as running_max_quantity_band
    from kept_rows
    where quantity = 8
),
filtered as (
    -- Keep recent, valid rows
    select
        id,
        quantity,
        unit_price,
        running_min_quantity,
        round(running_avg_quantity, 2) as running_avg_quantity_rounded
    from valid
    where event_seq >= 5
),
valid_order_items as (
    -- Filter the raw rows and derive helper columns
    select *, running_min_quantity * running_avg_quantity_rounded as running_min_quantity_times_running_avg_quantity_rounded
    from filtered
    where unit_price between 100 and 330
)
select quantity, running_avg_quantity_rounded
from valid_order_items;
