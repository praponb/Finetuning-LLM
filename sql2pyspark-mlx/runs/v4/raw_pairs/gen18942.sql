with cleaned as (
    -- Keep recent, valid rows
    select *, day(last_restock) last_restock_day
    from inventory
    where last_restock >= date '2023-09-12'
),
with_dims as (
    -- Attach products attributes
    select
        e.*,
        p.price,
        p.product_name,
        p.category
    from cleaned e
    left join products p
        on e.product_id = p.id
),
remaining_keys as (
    -- Set difference on keys
    select product_id, store_id
    from with_dims
    except distinct
    select product_id, store_id
    from inventory
    where store_id in (3, 5, 6)
),
reconciled as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from with_dims s
    inner join remaining_keys k on s.product_id = k.product_id and s.store_id = k.store_id
)
select
    store_id,
    product_id,
    quantity,
    last_restock,
    price,
    product_name,
    category,
    case when category rlike '^(Grocery|Books)$' then 'GROUP_B' when category rlike '^(Electronics|Home)$' then 'PRIORITY' else 'EXTENDED' end as category_zone
from reconciled
order by category nulls first;
