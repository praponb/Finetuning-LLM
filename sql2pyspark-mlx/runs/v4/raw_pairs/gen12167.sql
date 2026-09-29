with scored as (
    select
        *,
        round(cost / sum(cost) over (partition by category, supplier_id), 4) as cost_share,
        last_value(stock_qty, true) over (partition by supplier_id order by launch_date, id rows between unbounded preceding and current row) as last_known_stock_qty,
        ntile(10) over (partition by category, brand order by stock_qty, id) as stock_qty_bucket,
        first_value(stock_qty, true) over (partition by supplier_id, brand order by stock_qty, id rows between unbounded preceding and current row) as first_known_stock_qty
    from products
)
select
    s.id,
    s.product_name,
    s.category,
    s.brand,
    s.price,
    s.cost,
    s.stock_qty,
    s.launch_date,
    s.supplier_id,
    case when product_name rlike '^(Desk Lamp|Yoga Mat)$' then 'GROUP_A' when product_name rlike '^(Laptop Pro 14|wireless mouse)$' then 'GROUP_B' else 'PRIORITY' end as product_name_zone,
    (select max(t2.stock_qty) from products t2 where t2.supplier_id = s.supplier_id) as max_product_stock_qty
from scored s
order by max_product_stock_qty, price;
