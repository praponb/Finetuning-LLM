with deduped as (
    -- Deduplicate with ROW_NUMBER
    select *
    from (
        select *, row_number() over (partition by product_id order by last_restock desc, id) as row_num
        from inventory
    ) ranked
    where row_num = 1
),
deduped_inventory as (
    -- QUALIFY-style window filter
    select
        id,
        product_id,
        quantity,
        last_restock,
        row_num
    from deduped
    qualify row_number() over (partition by store_id order by last_restock, id) = 1
),
metrics as (
    -- Running and moving aggregates
    select
        *,
        last_value(quantity) ignore nulls over (order by quantity, id rows between unbounded preceding and current row) as last_known_quantity,
        ntile(3) over (order by quantity, id) as quantity_bucket
    from deduped_inventory
),
enriched as (
    -- Attach products attributes
    select a.*, p.price
    from metrics a
    left join products p
        on a.product_id = p.id
    where size(p.tags) >= 2
)
-- Join a dimension and aggregate
select
    d.category,
    count(*) as cnt,
    min(s.quantity_bucket) as min_quantity_bucket,
    sum(s.price) as total_price,
    grouping_id() as gid
from enriched s
join products d on s.product_id = d.id
group by rollup (d.category)
order by cnt nulls last
