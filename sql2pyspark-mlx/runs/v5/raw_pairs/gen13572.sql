with valid as (
    select *, greatest(stock_qty, cost) as greatest_stock_qty_cost
    from products
    where cost >= 640
),
with_latest as (
    select
        s.id,
        s.product_name,
        s.category,
        s.launch_date,
        s.tags,
        latest.inventory_last_restock,
        latest.inventory_quantity
    from valid s
    left join lateral (
            select c.last_restock as inventory_last_restock, c.quantity as inventory_quantity
            from inventory c
            where c.product_id = s.id
            order by c.quantity desc, c.id desc
            limit 1
        ) latest on true
),
scored as (
    select
        *,
        sum(inventory_quantity) over w_running as running_inventory_quantity,
        row_number() over (order by inventory_last_restock desc, id) as seq_no
    from with_latest
    where inventory_quantity != 116
    window w_running as (partition by category order by inventory_quantity, id rows between unbounded preceding and current row)
),
latest as (
    select *
    from scored
    qualify rank() over (partition by category order by seq_no desc) = 1
),
eligible as (
    select s.*
    from latest s
    where not exists (
            select 1
            from order_items ch
            where ch.product_id = s.id
              and ch.order_id in (27, 39, 40)
        )
),
with_last as (
    select
        s.*,
        x.quantity,
        x.inventory_last_restock_2
    from eligible s
    left join lateral (
            select c.quantity, c.last_restock as inventory_last_restock_2
            from inventory c
            where c.product_id = s.id
            order by c.quantity desc, c.id desc
            limit 1
        ) x on true
),
with_last_products as (
    select
        s.id,
        s.category,
        s.launch_date,
        s.tags,
        s.inventory_last_restock,
        s.running_inventory_quantity,
        s.seq_no,
        s.quantity,
        x.order_item_quantity
    from with_last s
    join lateral (
            select c.quantity as order_item_quantity
            from order_items c
            where c.product_id = s.id
            order by c.quantity desc, c.id desc
            limit 1
        ) x on true
)
select
    date_trunc('week', inventory_last_restock) as week_start,
    case when order_item_quantity >= 8 then 'HIGH' else 'LOW' end as order_item_quantity_tier,
    size(collect_set(category)) as num_categories,
    size(collect_set(category)) as num_categories_2
from with_last_products
group by date_trunc('week', inventory_last_restock), case when order_item_quantity >= 8 then 'HIGH' else 'LOW' end
having size(collect_set(category)) > 1
order by order_item_quantity_tier;
