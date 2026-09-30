with cleaned as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        store_id,
        product_id,
        quantity,
        last_restock,
        date_format(last_restock, 'yyyy-MM') as last_restock_label
    from inventory
    where store_id = 5
),
kept_keys as (
    -- Set difference on keys
    select product_id, id
    from cleaned
    except distinct
    select product_id, id
    from inventory
    where product_id in (2, 7)
),
kept_rows as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from cleaned s
    join kept_keys k on s.product_id = k.product_id and s.id = k.id
),
metrics as (
    -- Running and moving aggregates
    select
        *,
        avg(quantity) over (partition by store_id, product_id) as store_id_avg_quantity,
        cume_dist() over (partition by product_id order by quantity desc) as quantity_cume_dist
    from kept_rows
    where last_restock < '2025-03-01'
),
joined as (
    -- Attach products attributes
    select
        src.id,
        src.store_id,
        p.price
    from metrics src
    left join products p
        on src.product_id = p.id
),
metrics_inventory as (
    -- Running and moving aggregates
    select
        *,
        rank() over (partition by store_id order by price asc) as price_rank,
        round(price / sum(price) over (partition by store_id), 4) as price_share
    from joined
)
-- Join a dimension and aggregate
select
    p.region,
    count(*) as cnt,
    min(s.price_share) as min_price_share
from metrics_inventory s
join stores p on s.store_id = p.id
group by p.region;
