with ranked as (
    select *, row_number() over (partition by region order by opened_date desc, id) as dedup_rank
    from stores
    qualify dedup_rank = 1
),
scored as (
    select
        id,
        store_name,
        city,
        region,
        opened_date,
        sqft,
        dedup_rank,
        ntile(5) over (order by sqft desc, id) as sqft_bucket,
        lag(sqft, 2) over (partition by region, city order by sqft, id) as prev_sqft
    from ranked
),
latest_match as (
    select
        s.*,
        top1.inventory_quantity,
        top1.inventory_last_restock
    from scored s
    left join lateral (
            select c.quantity as inventory_quantity, c.last_restock as inventory_last_restock
            from inventory c
            where c.store_id = s.id
            order by c.quantity desc, c.id desc
            limit 1
        ) top1 on true
),
not_flagged as (
    select s.*
    from latest_match s
    where exists (
            select 1
            from inventory ch
            where ch.store_id = s.id
              and ch.last_restock < s.opened_date
        )
)
select
    region,
    sum(inventory_quantity) filter (where region = 'South') as south_total_inventory_quantity,
    size(collect_set(city)) as num_cities,
    max(inventory_quantity) filter (where region = 'South') as south_max_inventory_quantity
from not_flagged
group by region
having south_total_inventory_quantity > 0;
