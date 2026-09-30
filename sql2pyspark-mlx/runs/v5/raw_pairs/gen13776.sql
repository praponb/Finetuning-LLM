with valid as (
    -- Keep recent, valid rows
    select *, dayofweek(last_restock) as last_restock_dayofweek
    from inventory
    where last_restock >= '2025-06-01'
),
enriched as (
    -- Running and moving aggregates
    select
        id,
        store_id,
        quantity,
        lead(last_restock_dayofweek, 2) over (partition by product_id order by quantity asc, id asc) as next_last_restock_dayofweek,
        rank() over (partition by product_id order by last_restock_dayofweek asc nulls last) as last_restock_dayofweek_rank,
        row_number() over (partition by product_id order by last_restock_dayofweek asc, id asc) as row_seq,
        cume_dist() over (partition by store_id order by quantity desc nulls first) as quantity_cume_dist
    from valid
),
enriched_inventory as (
    -- Attach stores attributes
    select
        a.id,
        a.store_id,
        a.quantity,
        a.row_seq,
        a.quantity_cume_dist,
        d.opened_date,
        d.city
    from enriched a
    left join stores d
        on a.store_id = d.id
),
latest as (
    -- Keep one row per key (latest first)
    select *
    from enriched_inventory
    qualify row_number() over (partition by city order by opened_date desc nulls last, id asc) = 1
),
joined as (
    -- Attach stores attributes
    select
        s.id,
        s.store_id,
        s.quantity,
        s.city,
        b.opened_date as store_opened_date,
        b.sqft,
        b.store_name
    from latest s
    left join stores b
        on s.store_id = b.id
    where b.store_name is null
),
with_dims as (
    -- Attach stores attributes
    select
        a.id,
        a.store_id,
        a.store_opened_date,
        a.store_name,
        s.sqft as store_sqft
    from joined a
    left join stores s
        on a.store_id = s.id
),
latest_inventory as (
    -- Keep one row per key (latest first)
    select *, row_number() over (partition by store_id order by store_sqft desc nulls last, id asc) as dedup_rank
    from with_dims
    qualify dedup_rank = 1
)
-- Join a dimension and aggregate
select
    p.region,
    count(*) as rows_total,
    max(s.store_sqft) as max_store_sqft,
    grouping_id() as grouping_level
from latest_inventory s
inner join stores p on s.store_id = p.id
group by rollup (p.region)
