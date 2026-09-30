with enriched as (
    select
        id,
        product_id,
        sum(quantity) over w_running as running_sum_quantity,
        max(quantity) over w_running as running_max_quantity,
        min(quantity) over w_running as running_min_quantity,
        count(quantity) over w_running as running_count_quantity,
        max(quantity) over w_recent as product_id_max_quantity
    from inventory
    window w_running as (partition by store_id order by quantity, id rows between unbounded preceding and current row), w_recent as (partition by product_id)
),
with_dims as (
    select
        a.id,
        a.product_id,
        a.running_count_quantity,
        b.price,
        b.category,
        b.product_name
    from enriched a
    left join products b
        on a.product_id = b.id
),
remaining_keys as (
    select product_id, id
    from with_dims
    except distinct
    select product_id, id
    from inventory
    where product_id = 4
),
verified as (
    select s.*
    from with_dims s
    inner join remaining_keys k on s.product_id = k.product_id and s.id = k.id
),
joined as (
    select
        e.*,
        p.category as product_category,
        p.brand,
        p.stock_qty
    from verified e
    left join products p
        on e.product_id = p.id
)
select
    s.id,
    s.product_id,
    s.running_count_quantity,
    s.price,
    s.category,
    s.product_name,
    s.product_category,
    case when product_category rlike '^(Fashion|Grocery)$' then 'OTHER_ZONE' when product_category rlike '^(Electronics|Home)$' then 'GROUP_A' else 'PRIORITY' end as product_category_zone,
    (select sum(i2.quantity) from inventory i2 where i2.product_id = s.product_id) as sum_inventory_quantity
from joined s
order by sum_inventory_quantity
