with base as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        store_id,
        product_id,
        last_restock,
        date_sub(last_restock, 47) as last_restock_minus_47d
    from inventory
    where quantity between 155 and 181
),
with_dims as (
    -- Attach products attributes
    select s.*, d.cost
    from base s
    left join products d
        on s.product_id = d.id
),
metrics as (
    -- Running and moving aggregates
    select
        id,
        store_id,
        product_id,
        avg(cost) over (partition by store_id order by last_restock, id rows between 2 preceding and current row) as cost_avg_2,
        round(cost / sum(cost) over (partition by store_id), 4) as cost_share,
        avg(cost) over (order by last_restock_minus_47d, id rows between 3 preceding and current row) as cost_avg_3,
        row_number() over w_running as event_seq
    from with_dims
    window w_running as (partition by product_id order by cost, id)
),
with_dims_inventory as (
    -- Attach products attributes
    select
        e.*,
        d.product_name,
        d.stock_qty
    from metrics e
    inner join products d
        on e.product_id = d.id
    where length(d.product_name) < 12
)
select
    s.id,
    s.store_id,
    s.product_id,
    s.cost_avg_2,
    s.cost_share,
    s.event_seq,
    s.stock_qty,
    (select sum(o2.unit_price) from order_items o2 where o2.product_id = s.product_id) as sum_order_item_unit_price
from with_dims_inventory s;
