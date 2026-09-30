with flattened as (
    -- Unnest the array into one row per element
    select
        id,
        item.product_id,
        item.qty,
        item.promo
    from carts
    lateral view explode(items) li as item
),
kept_keys as (
    -- Set difference on keys
    select id
    from flattened
    except distinct
    select id
    from carts
    where created_at >= '2025-03-01'
),
verified as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from flattened s
    inner join kept_keys k on s.id = k.id
),
scored as (
    -- Window metrics over several different partitions
    select
        *,
        dense_rank() over (order by product_id nulls last) as product_id_dense_rank,
        round(qty / sum(qty) over (partition by promo), 4) as qty_share
    from verified
)
select
    product_id,
    qty,
    qty_share
from scored
order by qty desc nulls last;
