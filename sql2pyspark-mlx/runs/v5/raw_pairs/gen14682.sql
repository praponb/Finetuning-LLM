with with_last as (
    -- Latest matching inventory per row (LATERAL ... LIMIT 1)
    select s.*, top1.inventory_last_restock
    from stores s
    join lateral (
            select c.last_restock as inventory_last_restock
            from inventory c
            where c.store_id = s.id
            order by c.last_restock desc, c.id desc
            limit 1
        ) top1 on true
),
latest_match as (
    -- Latest matching inventory per row (LATERAL ... LIMIT 1)
    select
        s.id,
        s.city,
        s.opened_date,
        s.sqft,
        x.last_restock,
        x.inventory_quantity
    from with_last s
    left join lateral (
            select c.last_restock, c.quantity as inventory_quantity
            from inventory c
            where c.store_id = s.id
            order by c.quantity desc, c.id desc
            limit 1
        ) x on true
),
kept as (
    -- Anti-join via a correlated subquery
    select s.*
    from latest_match s
    where not exists (
            select 1
            from orders c
            where c.store_id = s.id
              and c.customer_id in (18, 21)
        )
),
enriched as (
    -- Window metrics over several different partitions
    select
        id,
        opened_date,
        sqft,
        ntile(5) over (partition by city order by sqft, id) as sqft_bucket,
        avg(inventory_quantity) over (order by opened_date, id rows between 3 preceding and current row) as inventory_quantity_avg_3,
        ntile(5) over (partition by city order by sqft, id) as sqft_bucket_2,
        sum(sqft) over (partition by city order by inventory_quantity, id rows between unbounded preceding and current row) as cumulative_sqft,
        percent_rank() over (partition by city order by inventory_quantity desc nulls last) as inventory_quantity_percent_rank
    from kept
),
valid as (
    -- Basic cleansing and defaults
    select *, round(inventory_quantity_avg_3, 0) as inventory_quantity_avg_3_rounded
    from enriched
    where opened_date <= '2025-01-01'
)
select
    id,
    opened_date,
    sqft,
    sqft_bucket,
    inventory_quantity_avg_3,
    sqft_bucket_2
from valid
order by inventory_quantity_avg_3, opened_date
