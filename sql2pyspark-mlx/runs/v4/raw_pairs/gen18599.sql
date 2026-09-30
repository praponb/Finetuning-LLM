with scored as (
    -- Ranking and gap features
    select
        id,
        store_name,
        city,
        region,
        opened_date,
        sqft,
        avg(sqft) over (partition by city order by opened_date asc, id asc rows between 5 preceding and current row) as sqft_avg_5,
        sum(sqft) over (order by opened_date asc, id asc rows between unbounded preceding and current row) as running_sqft
    from stores
),
not_flagged as (
    -- Semi-join via a correlated subquery
    select s.*
    from scored s
    where exists (
            select 1
            from inventory ch
            where ch.store_id = s.id
              and ch.quantity > 78
        )
),
with_latest as (
    -- Latest matching inventory per row (LATERAL ... LIMIT 1)
    select
        s.*,
        latest.inventory_quantity,
        latest.inventory_last_restock
    from not_flagged s
    left join lateral (
            select c.quantity as inventory_quantity, c.last_restock as inventory_last_restock
            from inventory c
            where c.store_id = s.id
            order by c.quantity desc, c.id desc
            limit 1
        ) latest on true
),
ranked as (
    -- Keep one row per key (latest first)
    select *
    from with_latest
    qualify row_number() over (partition by city order by inventory_last_restock desc nulls first, id asc) = 1
),
kept as (
    -- Anti-join via a correlated subquery
    select s.*
    from ranked s
    where not exists (
            select 1
            from orders ch
            where ch.store_id = s.id
              and ch.store_id = 20
        )
)
select
    id,
    store_name,
    city,
    region,
    sqft,
    sqft_avg_5,
    running_sqft,
    inventory_quantity,
    inventory_last_restock,
    case when store_name rlike '^(Airport Kiosk|Siam Flagship)$' then 'GROUP_A' when store_name rlike '^(Downtown Express|Mall Central)$' then 'CORE' else 'EXTENDED' end as store_name_zone
from kept
order by sqft_avg_5 desc, region asc
