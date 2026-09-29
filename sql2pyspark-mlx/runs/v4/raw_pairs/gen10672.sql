with ranked as (
    -- QUALIFY-style window filter
    select *
    from (
        select *, row_number() over (partition by product_id, store_id order by last_restock desc, id) as rn
        from inventory
    ) t
    where rn = 1
),
enriched as (
    -- Attach products attributes
    select a.*, p.category
    from ranked a
    left join products p
        on a.product_id = p.id
),
valid as (
    -- Filter the raw rows and derive helper columns
    select
        *,
        coalesce(quantity, 0) as quantity_filled,
        coalesce(category, 'DIRECT') as category_filled
    from enriched
    where last_restock > date '2023-07-27'
      and category is not null
)
select
    id,
    store_id,
    product_id,
    quantity,
    last_restock,
    category
from valid
order by last_restock desc;
